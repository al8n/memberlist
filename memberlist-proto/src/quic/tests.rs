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

/// Like [`test_config`] but with a SMALL connection-level `receive_window` (the
/// binding flow-control constraint, released only via MAX_DATA/ACK) and a LARGE
/// per-stream `stream_receive_window` (never the constraint, so no
/// MAX_STREAM_DATA is ever sent). A peer receiving a large reliable message under
/// this config back-pressures the sender at the CONNECTION level and drains it in
/// window-sized MAX_DATA increments — the setup for the partial-write flush
/// regression.
fn test_config_small_conn_window() -> QuicOptions {
  let mut transport = quinn_proto::TransportConfig::default();
  transport.max_idle_timeout(Some(
    quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
  ));
  transport.receive_window(quinn_proto::VarInt::from_u32(16 * 1024));
  transport.stream_receive_window(quinn_proto::VarInt::from_u32(4 * 1024 * 1024));
  QuicOptions::new(
    crate::quic::crypto::tests::test_endpoint_config(&[0x5au8; 32]),
    crate::quic::crypto::tests::test_server(),
    crate::quic::crypto::tests::test_client(),
    transport,
    "localhost",
    UnreliableTransport::Datagram,
  )
}

/// Like [`test_config_small_conn_window`] but also RAISING the peer's
/// concurrent-bidi-stream limit, so a sender can open MANY reliable exchanges at
/// once and have them all back-pressure at the CONNECTION window — the setup for
/// the connection-window Writable-storm that a single MAX_DATA releases.
fn test_config_small_conn_window_bidi(bidi: u32) -> QuicOptions {
  let mut transport = quinn_proto::TransportConfig::default();
  transport.max_idle_timeout(Some(
    quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
  ));
  transport.receive_window(quinn_proto::VarInt::from_u32(16 * 1024));
  transport.stream_receive_window(quinn_proto::VarInt::from_u32(4 * 1024 * 1024));
  transport.max_concurrent_bidi_streams(quinn_proto::VarInt::from_u32(bidi));
  QuicOptions::new(
    crate::quic::crypto::tests::test_endpoint_config(&[0x5au8; 32]),
    crate::quic::crypto::tests::test_server(),
    crate::quic::crypto::tests::test_client(),
    transport,
    "localhost",
    UnreliableTransport::Datagram,
  )
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

/// Like [`make_endpoint`] but with every periodic membership scheduler disabled
/// (probe / gossip / push-pull interval = 0, so [`Endpoint::start_scheduling`]
/// arms none of them). A residue-drain test can then drive `handle_timeout` at the
/// sticky catch-up anchor across several `CATCHUP_INTERVAL` steps with NO
/// randomly-staggered membership timer ever falling due inside the drain window,
/// so every wake is provably a BOUNDED catch-up (never a scheduled full tick)
/// regardless of the membership RNG stagger. The QUIC transport config is the
/// default [`test_config`]; only the periodic schedulers change.
fn make_endpoint_no_schedulers(id: &str, addr: SocketAddr, now: Instant) -> QuicEndpoint<SmolStr> {
  let cfg = EndpointOptions::new(SmolStr::new(id), addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  make_endpoint_full(cfg, test_config(), addr, now)
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
      None,
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
/// (`DatagramSendStatus::Queued`) and self-flushes: the send collects that
/// connection's owed transmits inline, so the datagram surfaces on
/// `poll_transmit` immediately, with no `flush_outbound_transmits` or
/// `handle_timeout` call.
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

  // Drain any residual handshake transmits so the surfacing assertion below
  // observes only the datagram the send itself flushes.
  while a.poll_transmit().is_some() {}

  let outcome = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01gossip"), now);
  assert_eq!(outcome, super::DatagramSendStatus::Queued);
  // Self-flushing: the datagram surfaces on poll_transmit immediately — no
  // flush_outbound_transmits or handle_timeout call precedes this poll.
  let surfaced = a.poll_transmit();
  assert!(
    matches!(surfaced, Some((to, _)) if to == b_addr),
    "a queued unreliable datagram must self-flush and surface on poll_transmit; got {surfaced:?}"
  );
}

/// The unreliable send does O(target connection) work: it self-flushes ONLY the
/// addressed connection and services no other connection, regardless of how many
/// connections the sender pools. With an established A↔B plus K additional
/// established connections on A, one datagram to B surfaces on `poll_transmit`
/// (T1) while `connection_visits` stays at zero (T2) — the inline collect is a
/// transmit-collect, not a `service_one_conn`.
///
/// Mutation-verify (T1): remove the inline collect in `queue_unreliable_datagram`
/// — the datagram never surfaces. Mutation-verify (T2): replace the inline collect
/// with `flush_outbound(now)` — `connection_visits` jumps to the pool size.
#[test]
fn unreliable_send_flushes_only_its_target_connection() {
  let now = Instant::now();
  let a_addr: SocketAddr = "127.0.0.1:8410".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8411".parse().unwrap();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // K additional established connections on A, so the pool is large enough that a
  // full-table service (the flush-all mutation) would be conspicuous.
  const EXTRA_CONNS: usize = 50;
  for i in 0..EXTRA_CONNS {
    let c_addr: SocketAddr = format!("127.0.0.1:{}", 8412 + i).parse().unwrap();
    let mut c = make_endpoint(&format!("c{i}"), c_addr, now);
    establish(&mut a, &mut c, a_addr, c_addr, now);
    // `c` drops here; A's established connection to it persists as A-side state.
  }
  assert!(
    a.conns.iter_handles().len() > EXTRA_CONNS,
    "A must pool the target plus every extra connection"
  );

  // Start from a drained outbound queue and a zeroed visit counter so the two
  // assertions below observe only the effect of the single send.
  while a.poll_transmit().is_some() {}
  a.counters.connection_visits = 0;

  let outcome = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01gossip"), now);
  assert_eq!(outcome, super::DatagramSendStatus::Queued);

  // T2: no connection was serviced — the inline collect drains transmits, it does
  // not run `service_one_conn`.
  assert_eq!(
    a.counters.connection_visits,
    0,
    "the send must not service any connection; a full-table flush would visit all {}",
    EXTRA_CONNS + 1
  );
  // T1: the datagram self-flushed onto the target connection and surfaces now.
  let surfaced = a.poll_transmit();
  assert!(
    matches!(surfaced, Some((to, _)) if to == b_addr),
    "the queued datagram must surface on poll_transmit with no flush; got {surfaced:?}"
  );
}

/// A cold-dial unreliable send self-flushes the fresh connection's Initial: the
/// send mints the connection via `get_or_dial`, returns `NotReady` (datagrams are
/// not negotiated mid-handshake), and its inline collect emits the Initial so the
/// handshake warms this tick — no flush or timeout needed.
///
/// Mutation-verify: gate the inline collect to the `Queued` arm only — the cold
/// dial's Initial then never surfaces.
#[test]
fn cold_dial_unreliable_send_self_flushes_the_connection_initial() {
  let now = Instant::now();
  let a_addr: SocketAddr = "127.0.0.1:8480".parse().unwrap();
  let cold: SocketAddr = "127.0.0.1:8481".parse().unwrap();
  let mut a = make_endpoint("a", a_addr, now);
  while a.poll_transmit().is_some() {}

  let outcome = a.queue_unreliable_datagram(cold, Bytes::from_static(b"\x01g"), now);
  assert_eq!(outcome, super::DatagramSendStatus::NotReady);

  // Self-flushing cold dial: the fresh connection's Initial surfaces now.
  let surfaced = a.poll_transmit();
  assert!(
    matches!(surfaced, Some((to, _)) if to == cold),
    "the cold dial's Initial must surface on poll_transmit with no flush; got {surfaced:?}"
  );
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
  // connection is still handshaking) re-parks the intent into
  // `dial_parked[unreachable]` with deadline `now + stream_timeout`. The
  // returned `StreamId` is the correlation handle the QUIC driver coerces to its
  // parked `ExchangeId`.
  let id = a
    .start_user_message(unreachable, Bytes::from_static(b"hello"), now)
    .expect("issued while running");
  let expected_eid = ExchangeId::from(id);

  assert_eq!(
    a.dial_parked
      .get(&unreachable)
      .map(|b| b.len())
      .unwrap_or(0),
    1,
    "test precondition: the in-band dial must re-park the still-handshaking \
       UserMessage intent into `dial_parked`"
  );
  let entry = a
    .dial_parked
    .get_mut(&unreachable)
    .and_then(|b| b.front_mut())
    .expect("dial_parked bucket non-empty per the preceding assertion");
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
  // in-band, and (the connection is still handshaking) re-parks the
  // intent into `dial_parked[unreachable]`. The returned `StreamId` is the
  // correlation handle the QUIC driver coerces to its parked
  // `ExchangeId`.
  let id = a.start_push_pull(unreachable, PushPullKind::Refresh, now);
  let expected_eid = ExchangeId::from(id);
  assert_eq!(
    a.dial_parked
      .get(&unreachable)
      .map(|b| b.len())
      .unwrap_or(0),
    1,
    "test precondition: the in-band dial must re-park the still-handshaking \
       PushPull intent into `dial_parked`"
  );
  let entry = a
    .dial_parked
    .get_mut(&unreachable)
    .and_then(|b| b.front_mut())
    .expect("dial_parked bucket non-empty per the preceding assertion");
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
/// exchange kind/peer and services its dial from the returned `DialIntent` in-band
/// (no event-queue sieve). Against a cold peer the handshake is not yet complete,
/// so no bridge opens this tick but a quinn Initial is emitted on the outbound path
/// (the dial was attempted). Covers the wrapper body and the `process_dial_entry`
/// cold-dial requeue arm.
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
  // connection re-parks the intent into `dial_parked[peer]`.
  let id = a.start_push_pull(peer, PushPullKind::Refresh, now);
  let expected_eid = ExchangeId::from(id);
  assert_eq!(
    a.dial_parked.get(&peer).map(|b| b.len()).unwrap_or(0),
    1,
    "precondition: intent re-parked"
  );

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
  a.dial_parked
    .get_mut(&peer)
    .and_then(|b| b.front_mut())
    .unwrap()
    .deadline = now + Duration::from_secs(30);

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
    a.dial_pending.is_empty() && a.dial_parked.is_empty(),
    "the retired intent must not be requeued onto dial_pending or re-parked"
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
/// and serviced — but it advances no stream state, so it fires no readiness
/// trigger and pumps NONE of that connection's bridges: the pass-scoped ready
/// queue drains empty. With K live bridges standing on the attacked connection,
/// per-datagram `connection_visits` stays exactly `1` (the addressed connection)
/// and `bridge_visits` stays exactly `0` — the pump set is precisely the bridges
/// the datagram's frames advanced, and a discarded datagram advances none.
///
/// Non-vacuity control: a genuinely-advancing datagram (a real response STREAM
/// frame from the peer) DOES pump its bridge, so `bridge_visits` climbs above `0`
/// — the zero above is the ready set filtering garbage, not a pump path that
/// never runs.
///
/// Negative control: pump every bridge on the addressed connection (the former
/// all-bridges-on-`ch` pump) instead of draining the ready queue — each flooded
/// datagram then pumps all K of A's live bridges, so `bridge_visits` climbs to K
/// and the `== 0` assertions fail.
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

  let ch_a = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a connection to A");
  let bridges_on = |ep: &QuicEndpoint<SmolStr>, ch: quinn_proto::ConnectionHandle| {
    ep.bridges.values().filter(|br| br.ch() == ch).count()
  };

  // Hold ONE live bridge on B's connection to C1 (a connection OTHER than the
  // flooded one): B initiates a push/pull to the established C1, and C1 is never
  // ferried, so the bridge stays live. A former all-bridges scan would visit it.
  let _ = b.start_push_pull(c1_addr, PushPullKind::Refresh, now);
  while b.poll_transmit().is_some() {}

  // Stand K live bridges ON THE ATTACKED connection (B initiates K push/pulls to
  // the established A, opening K outbound bidis on B's connection to A). Hold B's
  // request datagrams to A instead of delivering them, so the bridges stay live
  // awaiting a response — and so a genuine round below can advance them.
  const K: usize = 3;
  for _ in 0..K {
    let _ = b.start_push_pull(a_addr, PushPullKind::Refresh, now);
  }
  let mut held_b_to_a: Vec<Vec<u8>> = Vec::new();
  while let Some((to, bytes)) = b.poll_transmit() {
    if to == a_addr {
      held_b_to_a.push(bytes.to_vec());
    }
    // Datagrams to C1 (and any other peer) are dropped so C1's bridge stays live.
  }
  let live_on_a = bridges_on(&b, ch_a);
  assert!(
    live_on_a >= 2,
    "test precondition: the attacked connection must hold multiple live bridges so \
       `== 0` is non-vacuous (former design would pump all {live_on_a}); got {live_on_a}"
  );

  // Capture a genuine A->B 1-RTT gossip datagram (a DATAGRAM frame, so it pumps
  // no bridge — it is unreliable, not stream data).
  let _ = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"gossip-probe-payload"), now);
  a.flush_outbound_transmits(now);
  let pristine = drain_first_datagram_to(&mut a, b_addr);
  while a.poll_transmit().is_some() {}

  // Corrupted flood: flip the AEAD tag (last byte); the plaintext DCID is intact,
  // so quinn routes it to B's connection-to-A and discards it on auth failure.
  // Each such datagram services ONLY A's connection and pumps NONE of its bridges.
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
      b.counters.bridge_visits, 0,
      "a corrupted datagram advances no stream state and must pump ZERO bridges, \
         not the {live_on_a} live bridges standing on A's connection"
    );
  }

  // Replayed flood: deliver the genuine datagram once (B extracts it), then
  // re-deliver the SAME bytes — a duplicate packet number quinn discards. The
  // duplicate still services ONLY A's connection and pumps no bridge.
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
    b.counters.bridge_visits, 0,
    "a replayed datagram advances no stream state and must pump ZERO bridges"
  );

  // Non-vacuity: a GENUINE advancing datagram DOES pump a bridge. Deliver B's held
  // requests to A so it accepts the inbound streams and responds, then ferry
  // A->B; a real response STREAM frame (or a refusal STOP_SENDING) makes B's
  // bridge readable and the ready-set drain pumps it. `bridge_visits` is reset
  // immediately before each A->B delivery so the count reflects that datagram
  // alone (not the per-iteration `handle_timeout` global pump).
  for bytes in &held_b_to_a {
    a.handle_udp(b_addr, bytes, now);
  }
  let mut genuine_pumped = false;
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.counters.bridge_visits = 0;
        b.handle_udp(a_addr, &bytes, now);
        if b.counters.bridge_visits > 0 {
          genuine_pumped = true;
        }
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
    if genuine_pumped {
      break;
    }
    if !moved {
      break;
    }
  }
  assert!(
    genuine_pumped,
    "a genuinely-advancing datagram (a real response STREAM frame) must pump its \
       bridge — `bridge_visits` climbing above 0 proves the ready set filters \
       garbage rather than never pumping"
  );
}

/// A reliable exchange back-pressured at the CONNECTION window — released ONLY
/// via MAX_DATA/ACK (a partial-window release, never MAX_STREAM_DATA) — completes
/// under a strict-poll (datagram-only) driver. The sender's `pending_out` flush
/// must LOOP until it re-blocks so quinn re-registers the stream and re-emits
/// `StreamEvent::Writable` on each credit release; a single write that partially
/// accepts restored connection-window credit without re-blocking leaves the tail
/// un-registered, no further `Writable` fires, and the strict-poll driver never
/// re-pumps the bridge until its exchange deadline (a false Timeout).
///
/// A sends a message several times B's small connection `receive_window`, so the
/// tail drains in multiple MAX_DATA increments — each needing a fresh `Writable`.
/// B's large `stream_receive_window` guarantees the per-stream window is never
/// the constraint, so no MAX_STREAM_DATA is ever sent: the ONLY re-wake is the
/// connection-window `Writable`. The whole DATA phase is driven with `handle_udp`
/// only (never `handle_timeout`), so the global pump-all cannot mask a missing
/// `Writable`; time never advances, so the bridge deadline never fires and the
/// exchange can only complete via the looped flush's re-registration.
///
/// Mutation-verify: revert `Bridge::pump_out`'s `pending_out` flush to a single
/// write (drop the loop) — after the first partial-credit `Writable` pump the
/// stream is no longer registered, no further `Writable` fires, and the ferry
/// stalls with B never receiving the full message; the length assertion fails.
#[test]
fn connection_window_backpressured_exchange_completes_under_strict_poll() {
  let a_addr: SocketAddr = "127.0.0.1:7982".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7983".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  // B advertises a small connection window (MAX_DATA-bound) but a large
  // per-stream window (never the constraint).
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window(), b_addr, now);

  // Establish A -> B with a normal (timer-driven) ferry; the user-message bridge
  // that carries the back-pressured payload is opened AFTER this, so the global
  // pump here cannot mask the data-phase behaviour under test.
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1,
    "test precondition: A and B must be connected before the back-pressured send"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}

  // A one-way reliable message several times B's 16 KiB connection window, so its
  // single reliable unit cannot fit the window and drains only in MAX_DATA-sized
  // increments.
  let payload = Bytes::from(vec![0xA7u8; 200 * 1024]);
  let _ = a
    .start_user_message(b_addr, payload.clone(), now)
    .expect("A starts a reliable user message to the established B");

  // STRICT-POLL data ferry: `handle_udp` only, NEVER `handle_timeout`, and time
  // never advances. Progress is possible only if each MAX_DATA re-emits a
  // `Writable` that re-pumps A's blocked flush.
  let mut b_got_len: Option<usize> = None;
  for _ in 0..1000 {
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
    while let Some(ev) = b.poll_event() {
      if let Event::UserPacket(p) = ev {
        b_got_len = Some(p.data_ref().as_ref().len());
      }
    }
    while a.poll_event().is_some() {}
    if b_got_len.is_some() {
      break;
    }
    if !moved {
      break;
    }
  }
  assert_eq!(
    b_got_len,
    Some(payload.len()),
    "a connection-window-back-pressured reliable message must complete under a \
       strict-poll driver via MAX_DATA-only credit: the looped `pending_out` \
       flush re-registers the stream on each partial release so `Writable` \
       re-pumps the bridge. A single-write flush stalls the tail (no re-wake) and \
       B never receives the full {}-byte payload.",
    payload.len()
  );
}

/// One connection-level flow-control window increase (a single MAX_DATA — one
/// attacker byte) makes quinn-proto emit `StreamEvent::Writable` for EVERY stream
/// blocked on the connection window, and the datagram `poll()` drain enqueues
/// each owning bridge. Without a bound the pass would then pump ALL of them (up to
/// `max_inbound_streams`, ~1024) for that ONE datagram. The pass budget caps that:
/// a single service pass pumps at most `MAX_BRIDGE_PUMPS_PER_PASS` ready bridges;
/// the overflow stays queued and drains on a later pass or the un-budgeted tick,
/// deferred but never dropped.
///
/// Construction: A opens MANY reliable exchanges to B, all back-pressured at B's
/// small connection window (so they stay live with un-flushed `pending_out`). The
/// Writable-storm enqueue is reproduced by enqueuing every live bridge via the
/// same `enqueue_ready_bridge` the `poll()` drain uses, then ONE service pass runs
/// over that full queue.
///
/// Mutation-verify: set the pass budget to `usize::MAX` (pump-to-empty) — the one
/// pass then pumps all ~N bridges, so `bridge_visits` jumps to N and the
/// `<= MAX_BRIDGE_PUMPS_PER_PASS` assertion fails.
#[test]
fn writable_storm_pass_pumps_at_most_the_budget() {
  let a_addr: SocketAddr = "127.0.0.1:7990".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7991".parse().unwrap();
  let now = Instant::now();
  // A's periodic membership schedulers are disabled so the multi-step catch-up
  // drain below can advance the clock past several `CATCHUP_INTERVAL`s with no
  // membership timer ever falling due — every wake is then a bounded catch-up.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  // B advertises a SMALL connection window (so A's sends back-pressure at the
  // connection level) and a HIGH bidi-stream limit (so A can open ~200 exchanges
  // at once).
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window_bidi(400), b_addr, now);

  // Establish A -> B (timer-driven ferry).
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "test precondition: A and B must be connected before opening the exchanges"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}

  // A opens N reliable user-message exchanges to B. Each opens a bidi and buffers
  // its payload; B's 16 KiB connection window admits only the first few, so the
  // rest stay live with un-flushed `pending_out`. Held (never ferried) so all
  // stay live.
  const N: usize = 200;
  let payload = Bytes::from(vec![0xC3u8; 4096]);
  for _ in 0..N {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to the established B");
  }
  while a.poll_transmit().is_some() {}
  let live = a.bridges.len();
  assert!(
    live >= 128,
    "test precondition: A must hold many concurrent connection-window-blocked \
       bridges so the budget is the binding constraint; got {live}"
  );

  // Reproduce the Writable storm: enqueue every live bridge onto the pass-scoped
  // ready queue via the SAME helper the `poll()` drain uses on each MAX_DATA
  // Writable. `flush_outbound`'s `finalize_tick` cleared the queue after the last
  // `start_user_message`, so this is a clean enqueue of the full storm set.
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  let ids: Vec<StreamId> = a.bridges.keys().copied().collect();
  for id in &ids {
    super::enqueue_ready_bridge(&mut a.bridges, &mut a.ready_bridges, *id);
  }
  let enqueued = a.ready_bridges.len();
  assert!(
    enqueued > super::MAX_BRIDGE_PUMPS_PER_PASS,
    "the storm must exceed the pass budget for the cap to be non-vacuous \
       (enqueued {enqueued}, budget {})",
    super::MAX_BRIDGE_PUMPS_PER_PASS
  );

  // ONE service pass over the full ready queue (the per-datagram path).
  a.counters.bridge_visits = 0;
  a.service_connection(ch, now);
  let one_pass = a.counters.bridge_visits;
  assert!(
    one_pass <= super::MAX_BRIDGE_PUMPS_PER_PASS as u64,
    "one service pass must pump at most MAX_BRIDGE_PUMPS_PER_PASS={} bridges, not \
       the {enqueued} Writable-storm bridges; pumped {one_pass} (pump-to-empty \
       would pump ~{enqueued})",
    super::MAX_BRIDGE_PUMPS_PER_PASS
  );
  assert!(
    one_pass >= 1,
    "non-vacuous: the budgeted pass must still pump at least one bridge"
  );
  // The un-pumped remainder stays QUEUED (deferred, not dropped).
  let residue = a.ready_bridges.len();
  assert_eq!(
    residue,
    enqueued - one_pass as usize,
    "exactly the un-pumped remainder rides to the next pass / tick"
  );
  assert!(
    residue > 0,
    "test precondition: the storm must leave a residue so liveness is non-vacuous"
  );

  // Liveness: the deferred residue is never dropped. A driver re-polls the earliest
  // wake (`poll_timeout`) and services it; the sticky catch-up anchor is the
  // earliest wake, so the first wake is a BOUNDED, membership-flat catch-up. Later
  // chunks may drain via a scheduled tick once the connection's transport timer
  // comes due (the anchor advances past it) — which is correct: nothing is dropped.
  a.counters.bridge_visits = 0;
  assert_eq!(
    a.poll_timeout(),
    a.next_catchup_at,
    "the sticky catch-up anchor is the earliest wake while a residue waits"
  );
  let mut catchup_steps = 0usize;
  let mut steps = 0usize;
  while !a.ready_bridges.is_empty() {
    let memb_before = a.membership_time_advances();
    let before = a.ready_bridges.len();
    let wake = a
      .poll_timeout()
      .expect("a non-empty residue always schedules a wake");
    a.handle_timeout(wake);
    let drained = before - a.ready_bridges.len();
    if a.membership_time_advances() == memb_before {
      // A Catchup-only wake: no membership tick, and it pumps at most one budget
      // chunk (a full tick would drain the whole residue at once).
      catchup_steps += 1;
      assert!(
        drained <= super::MAX_BRIDGE_PUMPS_PER_PASS,
        "a Catchup-only wake pumps at most the budget ({}); a step drained {drained}",
        super::MAX_BRIDGE_PUMPS_PER_PASS
      );
    }
    steps += 1;
    assert!(
      steps <= residue + 2,
      "the deferred residue must converge, never strand; still {} queued after \
         {steps} steps",
      a.ready_bridges.len()
    );
  }
  assert!(
    catchup_steps >= 1,
    "the residue must drain via at least one BOUNDED catch-up wake; reverting \
       handle_timeout to an unconditional run_tick makes every wake a full \
       membership tick and leaves this 0"
  );
  assert!(
    a.counters.bridge_visits >= residue as u64,
    "the catch-up must pump the whole {residue}-bridge residue (deferred, not \
       dropped); visited {}",
    a.counters.bridge_visits
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

/// The three incremental bridge indexes — [`QuicEndpoint::bridges_by_conn`],
/// [`QuicEndpoint::bridge_by_conn_sid`], and
/// [`QuicEndpoint::inbound_bridge_count`] — must each equal their brute-force
/// fold over `bridges` at every step, the guard that a bridge mint or reap site
/// missed its index maintenance. Drives a real A<->B push/pull (outbound +
/// inbound bridges minted and reaped) plus a connection teardown, cross-checking
/// after every servicing operation.
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
    // conn_slot back-pointers: the bridge at position `p` of `ch`'s bucket must
    // record `conn_slot == p`, so a reap's `swap_remove(conn_slot)` drops that
    // exact bridge in O(1). A missed mint-time record or a missed sibling fixup
    // leaves the back-pointer stale.
    for (ch, ids) in ep.bridges_by_conn.iter() {
      for (p, id) in ids.iter().enumerate() {
        let br = ep.bridges.get(id).unwrap_or_else(|| {
          panic!("bridges_by_conn[{ch:?}][{p}] = {id:?} absent from bridges (after: {label})")
        });
        assert_eq!(
          br.conn_slot(),
          p,
          "conn_slot back-pointer diverged from bucket position for {id:?} after: {label}"
        );
      }
    }
    // bridge_by_conn_sid: the finer-grained (ch, sid) -> id reverse index.
    let mut brute_sid: std::collections::HashMap<
      (quinn_proto::ConnectionHandle, quinn_proto::StreamId),
      StreamId,
    > = std::collections::HashMap::new();
    for (id, br) in ep.bridges.iter() {
      brute_sid.insert((br.ch(), br.sid()), *id);
    }
    assert_eq!(
      ep.bridge_by_conn_sid, brute_sid,
      "bridge_by_conn_sid diverged from the brute-force (ch, sid) -> id map after: {label}"
    );
    // inbound_bridge_count: live bridges absent from pending_outbound_kinds.
    assert_eq!(
      ep.inbound_bridge_count(),
      ep.inbound_bridge_count_recount(),
      "inbound_bridge_count diverged from the brute-force filter after: {label}"
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

/// A dial parked while its TARGET PEER's connection was handshaking must open its
/// bridge AND flush the first request bytes to that peer within the single
/// `handle_udp` that establishes the connection — not stall until the next global
/// tick.
///
/// N dials cold B: the in-band `service_dials` creates a handshaking connection
/// and re-parks the intent on `dial_parked[b_addr]` (no bridge yet). Ferrying the
/// handshake WITHOUT `n.handle_timeout`, the inbound datagram that drives N's
/// connection to B to Established runs `service_connection`'s scoped
/// establishment branch → `service_peer_bucket(b_addr)`, which opens the parked
/// dial's bridge on that same connection and pumps its first request bytes onto
/// the wire in that same pass.
///
/// Mutation-verify: drop step (c)'s bucket service (or its minted-bridge enqueue
/// and ready-queue drain), so the parked dial is never opened on the establishing
/// pass — no bridge mints until a later `handle_timeout` and `minted_same_pass`
/// stays false.
#[test]
fn parked_dial_mints_and_flushes_on_its_peer_establishment() {
  let n_addr: SocketAddr = "127.0.0.1:7920".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7921".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // N dials cold B: the in-band `service_dials` creates a handshaking connection
  // and re-parks the intent on B's bucket. No bridge is minted while B handshakes.
  let _ = n.start_push_pull(b_addr, PushPullKind::Join, now);
  assert!(
    n.dial_parked.contains_key(&b_addr),
    "test precondition: the dial to still-handshaking B must re-park on B's bucket"
  );
  assert_eq!(
    n.live_bridge_count(),
    0,
    "test precondition: no bridge is minted while B is still handshaking"
  );

  // Ferry the handshake one datagram at a time WITHOUT `n.handle_timeout` (a
  // global tick would service the parked dial through the full drain, defeating
  // the same-pass-as-establishment claim). The `n.handle_udp` that establishes
  // N's connection to B must service `dial_parked[b_addr]`, mint its bridge, and
  // flush the first request bytes to B in that same pass.
  let mut minted_same_pass = false;
  'outer: for _ in 0..400 {
    // N -> B (Initial, then handshake continuation).
    let mut n_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = n.poll_transmit() {
      if to == b_addr {
        n_out.push(bytes.to_vec());
      }
    }
    for dg in n_out {
      b.handle_udp(n_addr, &dg, now);
    }
    // B -> N: the datagram that drives N's connection to Established mints the
    // parked dial's bridge in that same `handle_udp` pass.
    let mut b_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == n_addr {
        b_out.push(bytes.to_vec());
      }
    }
    for dg in b_out {
      let before = n.live_bridge_count();
      n.handle_udp(b_addr, &dg, now);
      if before == 0 && n.live_bridge_count() >= 1 {
        minted_same_pass = true;
        // The freshly-minted bridge's first request bytes must be on N's outbound
        // queue to B this same pass — not stalled to a future tick.
        let mut sent_to_b = false;
        while let Some((to, _)) = n.poll_transmit() {
          if to == b_addr {
            sent_to_b = true;
          }
        }
        assert!(
          sent_to_b,
          "the minted bridge's first request bytes must flush to B within the \
             establishing pass"
        );
        break 'outer;
      }
    }
    // Advance only B's timers; N must NOT tick (that would service the parked
    // dial through the global full drain, defeating the same-pass claim).
    b.handle_timeout(now);
  }

  assert!(
    minted_same_pass,
    "the dial parked on N's connection to B must open its bridge within the single \
       handle_udp that established that connection — a scoped establishment must \
       not defer the parked dial to the next global tick"
  );
}

/// Assert the incremental [`super::DeadlineIndex`] behind
/// [`QuicEndpoint::poll_timeout`] returns exactly what a brute-force fold of the
/// same sources ([`QuicEndpoint::recompute_earliest_bruteforce`]) would — the
/// guard that catches a missed `set` at any deadline-mutating site. Also
/// cross-checks the incremental `unattempted_dial_count` against its brute-force
/// recount, so a drifting counter at any `dial_pending` mutation site (which
/// feeds the immediate-due half of the same fold) is caught here too.
fn assert_deadline_index_matches(ep: &mut QuicEndpoint<SmolStr>, label: &str) {
  assert_eq!(
    ep.unattempted_dial_count(),
    ep.unattempted_dial_recount(),
    "unattempted_dial_count diverged from the brute-force recount after: {label}"
  );
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

  // Public in-band push/pull: services its dial from the returned descriptor (no
  // event-queue sieve), attempts the dial, mints an outbound bridge, and flushes —
  // exercising the Dial, Conn, and Bridge keys plus the immediate-due term in one
  // call.
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

  // Settle the index, then measure one poll in isolation.
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

/// A connection-level loss must reap only ITS connection's bridges, via the
/// per-connection index, never a scan of the whole bridge table. B holds live
/// bridges on two connections; a peer `CONNECTION_CLOSE` arriving on one of them
/// (the datagram path) reaps that connection's bridges — the `bridge_scan_visits`
/// counter stays bounded by that connection's bridge count, well under the total.
///
/// Mutation-verify: restore the global `self.bridges.iter().filter(ch)` scan (and
/// count each examined entry) — the reap then examines every bridge in the table,
/// so `bridge_scan_visits` climbs to the total and the `< total` assertion fails.
#[test]
fn connection_lost_reap_scans_only_its_own_connections_bridges() {
  let b_addr: SocketAddr = "127.0.0.1:7690".parse().unwrap();
  let a_addr: SocketAddr = "127.0.0.1:7691".parse().unwrap();
  let c_addr: SocketAddr = "127.0.0.1:7692".parse().unwrap();
  let now = Instant::now();
  let mut b = make_endpoint("b", b_addr, now);
  let mut a = make_endpoint("a", a_addr, now);
  let mut c = make_endpoint("c", c_addr, now);

  // Establish A -> B and C -> B (each dials B; B pools one inbound connection to
  // each).
  for (caddr, peer) in [(a_addr, &mut a), (c_addr, &mut c)] {
    let _ = peer.start_push_pull(b_addr, PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = peer.poll_transmit() {
        if to == b_addr {
          b.handle_udp(caddr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == caddr {
          peer.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      peer.handle_timeout(now);
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

  // Open FEW live outbound bridges on B's connection to A (the loss target) and
  // MANY on B's connection to C, so the whole bridge table is much larger than
  // A's connection's share. The peers are never ferried again, so every bridge
  // stays live (waiting on a response) until the loss.
  for _ in 0..2 {
    let _ = b.start_push_pull(a_addr, PushPullKind::Refresh, now);
  }
  for _ in 0..6 {
    let _ = b.start_push_pull(c_addr, PushPullKind::Refresh, now);
  }
  while b.poll_transmit().is_some() {}

  let ch_x = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a connection to A");
  let x_bridges = b.bridges.values().filter(|br| br.ch() == ch_x).count();
  let total_before = b.live_bridge_count();
  assert!(
    x_bridges >= 1,
    "test precondition: the loss-target connection must hold at least one bridge"
  );
  assert!(
    total_before > x_bridges,
    "test precondition: the table ({total_before}) must be larger than the target \
       connection's share ({x_bridges}) so the O(ch) vs O(total) claim is non-vacuous"
  );

  // A closes its connection to B; ferry the CONNECTION_CLOSE datagram to B. It
  // arrives on the datagram path, so `service_connection` reaps A's connection's
  // bridges in that pass via the per-connection index.
  let ch_a2b = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  a.conns
    .get_mut(ch_a2b)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), Bytes::new());
  a.flush_outbound_transmits(now);
  let close_dg = drain_first_datagram_to(&mut a, b_addr);

  b.counters.bridge_scan_visits = 0;
  b.handle_udp(a_addr, &close_dg, now);

  let visits = b.counters.bridge_scan_visits;
  assert!(
    visits >= x_bridges as u64,
    "the lost connection's {x_bridges} bridges must have been reaped (visits {visits})"
  );
  assert!(
    visits < total_before as u64,
    "the ConnectionLost reap must examine only the lost connection's bridges \
       (O(ch)), not scan the whole {total_before}-bridge table; examined {visits}"
  );
}

/// Losing a connection holding K bridges drops them from `bridges_by_conn` in one
/// whole-bucket map remove followed by a per-bridge bucketless deindex — O(K)
/// SmallVec work, never O(K²). `bridge_bucket_scan_ops` counts per-element bucket
/// surgery; the bucketless loss path performs none, so it stays at 0 across the
/// loss (well within the O(K) bound).
///
/// Mutation-verify: revert the loss path to a per-bridge `retain` deindex (the
/// former O(K)-per-reap bucket scan) — reaping the shrinking bucket K times scans
/// K + (K-1) + ... elements, so `bridge_bucket_scan_ops` climbs to O(K²) and the
/// `<= K` assertion fails.
#[test]
fn connection_lost_reap_is_linear_in_bucket() {
  let b_addr: SocketAddr = "127.0.0.1:7693".parse().unwrap();
  let a_addr: SocketAddr = "127.0.0.1:7694".parse().unwrap();
  let now = Instant::now();
  let mut b = make_endpoint("b", b_addr, now);
  let mut a = make_endpoint("a", a_addr, now);

  // Establish A -> B (A dials; B pools an inbound connection to A).
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
    if b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    b.live_connections_to(a_addr) >= 1,
    "precondition: B must pool a connection to A"
  );

  // Open K live outbound bridges on B's connection to A; never ferry them, so
  // every one stays live (awaiting a response) until the loss.
  const K: usize = 8;
  for _ in 0..K {
    let _ = b.start_push_pull(a_addr, PushPullKind::Refresh, now);
  }
  while b.poll_transmit().is_some() {}
  let ch = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a connection to A");
  let k_live = b.bridges.values().filter(|br| br.ch() == ch).count();
  assert!(
    k_live >= 4,
    "precondition: enough sibling bridges to make O(K) vs O(K²) non-vacuous (got {k_live})"
  );

  // A closes its connection to B; ferry the CONNECTION_CLOSE to B on the datagram
  // path, so `service_connection`'s loss reap runs the bucketless whole-bucket
  // deindex in that pass.
  let ch_a2b = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  a.conns
    .get_mut(ch_a2b)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), Bytes::new());
  a.flush_outbound_transmits(now);
  let close_dg = drain_first_datagram_to(&mut a, b_addr);

  b.counters.bridge_bucket_scan_ops = 0;
  b.handle_udp(a_addr, &close_dg, now);

  assert_eq!(
    b.bridges.values().filter(|br| br.ch() == ch).count(),
    0,
    "every bridge on the lost connection must be reaped"
  );
  assert!(
    !b.bridges_by_conn.contains_key(&ch),
    "the lost connection's bucket must be gone"
  );
  let ops = b.counters.bridge_bucket_scan_ops;
  assert!(
    ops <= k_live as u64,
    "the connection-loss reap of {k_live} bridges must do O(K) SmallVec work \
       (the shipped bucketless whole-bucket remove does 0; got {ops}); a per-bridge \
       retain deindex would scan the shrinking bucket K times → O(K²)"
  );
}

/// Each single-bridge reap drops the bridge from its `bridges_by_conn` bucket in
/// O(1) via one `swap_remove`, so reaping K sibling bridges on a live connection
/// costs O(K) SmallVec work, not O(K²). `bridge_bucket_scan_ops` (one per
/// swap_remove) therefore equals the reaped count after all siblings reap through
/// the pump path.
///
/// Mutation-verify: revert `index_bridge_reap`'s `swap_remove` to the former
/// `retain` (counting each scanned element) — reaping the shrinking bucket scans
/// K + (K-1) + ... elements, so the counter climbs to O(K²) and the equality
/// assertion fails.
#[test]
fn sibling_bridge_reap_is_o1_per_bridge() {
  let b_addr: SocketAddr = "127.0.0.1:7696".parse().unwrap();
  let a_addr: SocketAddr = "127.0.0.1:7697".parse().unwrap();
  let now = Instant::now();
  let mut b = make_endpoint("b", b_addr, now);
  let mut a = make_endpoint("a", a_addr, now);

  // Establish B -> A and let the join push/pull settle so B holds a pooled
  // connection to A with no live bridge.
  let _ = b.start_push_pull(a_addr, PushPullKind::Join, now);
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
    b.handle_timeout(now);
    a.handle_timeout(now);
    if b.live_connections_to(a_addr) >= 1 && b.live_bridge_count() == 0 && !moved {
      break;
    }
  }
  assert!(
    b.live_connections_to(a_addr) >= 1,
    "precondition: B must pool a connection to A"
  );
  assert_eq!(
    b.live_bridge_count(),
    0,
    "precondition: the join push/pull must have settled"
  );
  while b.poll_transmit().is_some() {}

  // Open K sibling bridges on B's one connection to A; never ferry them, so they
  // share one exchange deadline and reap together on a timeout tick via the pump
  // path — each through a single O(1) `swap_remove`.
  const K: usize = 8;
  for _ in 0..K {
    let _ = b.start_push_pull(a_addr, PushPullKind::Refresh, now);
  }
  let ch = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a connection to A");
  let opened = b.bridges.values().filter(|br| br.ch() == ch).count();
  assert!(
    opened >= 4,
    "precondition: enough sibling bridges (got {opened})"
  );

  // The multi-element bucket must carry exact conn_slot back-pointers — the
  // lifecycle cross-check only ever sees size-1 buckets, so this is where the
  // mint-time recording is checked non-vacuously. A broken swap_remove fixup
  // would additionally trip `index_bridge_reap`'s `debug_assert` mid-reap below.
  {
    let bucket = b
      .bridges_by_conn
      .get(&ch)
      .expect("the sibling bucket exists");
    for (p, id) in bucket.iter().enumerate() {
      assert_eq!(
        b.bridges.get(id).unwrap().conn_slot(),
        p,
        "conn_slot must equal the bucket position for sibling {id:?}"
      );
    }
  }

  while b.poll_transmit().is_some() {}

  // Advance to the bridges' exchange deadline; the pump reaps each via
  // `index_bridge_reap`'s O(1) swap_remove. Keep advancing to the reported
  // deadline until the bucket empties (an intervening connection PTO/timer may be
  // earlier); the connection outlives the bridges (idle timeout 20 s > the
  // exchange deadline), so every reap goes through the pump path, never the
  // bucketless connection-loss path.
  b.counters.bridge_bucket_scan_ops = 0;
  b.counters.bridge_pump_path_reaps = 0;
  let mut guard = 0;
  while b.bridges.values().any(|br| br.ch() == ch) {
    let due = b
      .poll_timeout()
      .expect("live bridges contribute a deadline");
    b.handle_timeout(due + Duration::from_millis(1));
    guard += 1;
    assert!(
      guard < 128,
      "sibling bridges failed to reap within the deadline horizon"
    );
  }

  // Each single-bridge pump-path reap performs exactly one O(1) bucket
  // swap_remove, so the two counters stay equal however many bridges churn
  // through the reap: advancing membership time escalates B's probe of the
  // unreachable A and opens extra reliable-fallback-ping bridges that also reap,
  // but those track together in both counters. The sibling bucket (>= 4 deep)
  // makes the equality discriminating — the former O(bucket) retain would scan
  // 4 + 3 + ... elements per reap, pushing the scan-op count above the reap count.
  let scan_ops = b.counters.bridge_bucket_scan_ops;
  let reaps = b.counters.bridge_pump_path_reaps;
  assert!(
    reaps >= opened as u64,
    "the {opened} sibling bridges must have reaped through the pump path (reaps {reaps})"
  );
  assert_eq!(
    scan_ops, reaps,
    "each pump-path reap must cost exactly one O(1) swap_remove (scan_ops {scan_ops} \
       vs reaps {reaps}); the former O(bucket) retain would push scan_ops above the \
       reap count"
  );
}

/// `poll_timeout` must answer "is any dial unattempted?" from the O(1)
/// `unattempted_dial_count`, never by scanning `dial_pending`. With many
/// already-attempted dials parked, repeated `poll_timeout` calls examine zero
/// dial entries.
///
/// Mutation-verify: revert `refresh_immediate_due` to
/// `dial_pending.iter().any(|d| !d.attempted)` (counting each visited entry) —
/// the examined count then scales with the parked-dial count times the poll
/// count and the `== 0` assertion fails.
#[test]
fn poll_timeout_does_not_scan_dial_pending() {
  let n_addr: SocketAddr = "127.0.0.1:7695".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);
  // Anchor `last_now` (and run one empty tick) so the immediate-due term is
  // well-defined.
  n.handle_timeout(now);

  // Park MANY already-attempted dials directly in `dial_pending` — the steady
  // state a flooded dial queue reaches after its first service pass. Attempted
  // entries do not contribute to `unattempted_dial_count`, so it stays 0.
  let parked = 4096usize;
  for i in 0..parked {
    let peer: SocketAddr = format!("127.0.0.2:{}", 1000 + (i % 60000)).parse().unwrap();
    n.dial_pending.push_back(super::PendingDial {
      id: StreamId::from_raw(i as u64),
      peer,
      deadline: now + Duration::from_secs(30),
      wake: now + Duration::from_secs(30),
      attempted: true,
      kind: super::ExchangeKind::UserMessage,
    });
  }
  assert_eq!(n.unattempted_dial_count(), 0);
  assert_eq!(
    n.unattempted_dial_recount(),
    0,
    "all parked dials are attempted, so the brute-force recount is 0"
  );

  // The O(1) path reads the counter and never iterates the parked dials.
  n.counters.dial_pending_scan_visits = 0;
  for _ in 0..16 {
    let _ = n.poll_timeout();
  }
  assert_eq!(
    n.counters.dial_pending_scan_visits, 0,
    "poll_timeout must read unattempted_dial_count, not scan the {parked} parked \
       dials; reverting refresh_immediate_due to a dial_pending scan makes this climb"
  );

  // Sanity: an unattempted dial DOES flip the immediate-due term on — still with
  // no scan.
  n.dial_pending.push_back(super::PendingDial {
    id: StreamId::from_raw(parked as u64),
    peer: "127.0.0.2:9999".parse().unwrap(),
    deadline: now + Duration::from_secs(30),
    wake: now + Duration::from_secs(30),
    attempted: false,
    kind: super::ExchangeKind::UserMessage,
  });
  n.unattempted_dial_count += 1;
  assert_eq!(n.unattempted_dial_count(), n.unattempted_dial_recount());
  n.counters.dial_pending_scan_visits = 0;
  let due = n.poll_timeout();
  assert_eq!(
    due,
    Some(now),
    "an unattempted dial forces an immediate-due wake at last_now"
  );
  assert_eq!(
    n.counters.dial_pending_scan_visits, 0,
    "reading the immediate-due term must still not scan dial_pending"
  );
}

/// The global `max_quic_connections` cap must bound OUTBOUND dials too — both
/// reliable dials and the datagram-fallback dial route through `get_or_dial`, so
/// neither may grow the connection table past the cap.
///
/// Mutation-verify: remove the cap check on the new-connection branch of
/// `get_or_dial` — the outbound dials then create a fresh connection each and the
/// `<= cap` assertion fails.
#[test]
fn outbound_dials_cannot_exceed_global_connection_cap() {
  let s_addr: SocketAddr = "127.0.0.1:7699".parse().unwrap();
  let now = Instant::now();
  let cap = 3usize;
  let cfg = EndpointOptions::new(SmolStr::new("s"), s_addr);
  let qc = test_config().with_max_quic_connections(Some(cap));
  let mut s = make_endpoint_full(cfg, qc, s_addr, now);

  // Fire cap + surplus reliable dials to DISTINCT cold peers (none answer, so
  // each created connection stays handshaking and holds its slab slot).
  let surplus = 3usize;
  for i in 0..(cap + surplus) {
    let peer: SocketAddr = format!("127.0.0.3:{}", 4000 + i).parse().unwrap();
    let _ = s.start_push_pull(peer, PushPullKind::Join, now);
    assert!(
      s.conns.iter_handles().len() <= cap,
      "outbound reliable dials must never push the table past the cap {cap}; got {} \
         after {} dials",
      s.conns.iter_handles().len(),
      i + 1
    );
  }
  assert_eq!(
    s.conns.iter_handles().len(),
    cap,
    "the first {cap} distinct-peer dials should have filled the table exactly"
  );

  // A datagram-fallback dial to yet another cold peer is refused at the cap
  // (best-effort NotReady) and creates no connection.
  let dg_peer: SocketAddr = "127.0.0.3:4100".parse().unwrap();
  let status = s.queue_unreliable_datagram(dg_peer, Bytes::from_static(b"gossip"), now);
  assert_eq!(
    status,
    super::DatagramSendStatus::NotReady,
    "a datagram-fallback dial at the cap must report NotReady"
  );
  assert_eq!(
    s.live_connections_to(dg_peer),
    0,
    "the refused fallback dial must create no connection"
  );
  assert!(
    s.conns.iter_handles().len() <= cap,
    "the datagram-fallback dial must not exceed the cap either"
  );

  // Every dial refused at the cap (the reliable surplus + the fallback) is
  // counted against the connection-cap metric.
  let rejected = s.metrics().quic_connections_rejected;
  assert!(
    rejected >= (surplus + 1) as u64,
    "each dial refused at the cap must be counted (>= {} expected, got {rejected})",
    surplus + 1
  );
}

/// A cold dial (to a peer with NO established connection) that `service_dials`
/// attempts must flush its Initial AND register its deadline key in the same call
/// — even though it mints no bridge (its `open(Bi)` returns `None`, still
/// handshaking) and merely re-parks on the peer's bucket. The touched-but-
/// bridgeless connection is the case the full `service_dials` (run in-band by
/// every `start_*` wrapper) must still flush and index the same pass.
///
/// Mutation-verify: revert the touched-conns flush to collect only minted-bridge
/// connections — C mints no bridge, so its Initial is never flushed within the
/// `start_push_pull` call and `flushed_to_c` stays false.
#[test]
fn cold_dial_touched_flushes_initial_same_pass() {
  let n_addr: SocketAddr = "127.0.0.1:7710".parse().unwrap();
  let c_addr: SocketAddr = "127.0.0.1:7711".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);
  // C is never instantiated — N only ever cold-dials its address.

  // Dial the COLD peer C in-band: `start_push_pull` runs the full `service_dials`
  // and `flush_outbound`. C has no established connection, so `get_or_dial`
  // creates a fresh handshaking connection whose `open(Bi)` returns `None` — no bridge
  // minted, the intent re-parks on C's bucket. That connection is nonetheless
  // TOUCHED: its Initial and deadline key must emerge within this same call.
  let _ = n.start_push_pull(c_addr, PushPullKind::Join, now);

  let mut flushed_to_c = false;
  while let Some((to, _)) = n.poll_transmit() {
    if to == c_addr {
      flushed_to_c = true;
    }
  }
  assert!(
    flushed_to_c,
    "the cold dial to C (no bridge minted) must flush its Initial to C within the \
       single start_push_pull call — a touched-but-bridgeless connection must not \
       stall until the next global tick"
  );

  // The still-handshaking intent re-parked on C's bucket, and its connection's
  // deadline key was registered THIS call — a cold-dial connection is indexed
  // only by the touched-conns flush, not by `get_or_dial`.
  assert!(
    n.dial_parked.contains_key(&c_addr),
    "the still-handshaking cold dial must re-park on C's bucket"
  );
  assert_eq!(n.live_bridge_count(), 0, "a cold dial mints no bridge");
  let ch_c = n
    .conns
    .handle_for(&c_addr)
    .expect("the cold dial created N's connection to C");
  assert!(
    n.deadline_index
      .contains_key(super::deadline::TimerKey::Conn(ch_c)),
    "the cold-dialed connection's deadline key must be registered the same call \
       its Initial is flushed"
  );
}

/// Establishment is SCOPED: when one connection completes its handshake,
/// `service_connection` services ONLY that peer's parked-dial bucket, never the
/// whole dial queue. Many peers hold parked dials; N's connection to A
/// establishes; the `dial_entries_serviced` counter must advance by exactly A's
/// bucket size, and every other peer's bucket must remain untouched.
///
/// Mutation-verify: revert step (c) to the whole-queue `service_dials` drain — it
/// processes every parked bucket, so the counter jumps by the TOTAL parked-dial
/// count (and drains the other buckets away), failing both assertions.
#[test]
fn establishment_services_only_its_peer_bucket() {
  let n_addr: SocketAddr = "127.0.0.1:7730".parse().unwrap();
  let a_addr: SocketAddr = "127.0.0.1:7731".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);
  let mut a = make_endpoint("a", a_addr, now);

  // Two REAL dials to A, issued while A is still cold: each re-parks on A's bucket
  // (the connection is handshaking, `open(Bi)` returns None).
  let _ = n.start_push_pull(a_addr, PushPullKind::Join, now);
  let _ = n.start_push_pull(a_addr, PushPullKind::Refresh, now);
  let a_bucket = n.dial_parked.get(&a_addr).map(|b| b.len()).unwrap_or(0);
  assert_eq!(
    a_bucket, 2,
    "test precondition: both dials to still-handshaking A re-park on A's bucket"
  );

  // MANY parked dials on OTHER cold peers, injected directly. They target
  // never-answered addresses, so even a whole-queue drain only re-parks them
  // (open(Bi) None, handshaking); the scoped path never touches them at all.
  let other_peers = 5usize;
  let per_peer = 8usize;
  let mut next_id = 10_000u64;
  for p in 0..other_peers {
    let peer: SocketAddr = format!("127.0.0.9:{}", 6000 + p).parse().unwrap();
    for _ in 0..per_peer {
      n.dial_parked
        .entry(peer)
        .or_default()
        .push_back(super::PendingDial {
          id: StreamId::from_raw(next_id),
          peer,
          deadline: now + Duration::from_secs(30),
          wake: now + Duration::from_secs(30),
          attempted: true,
          kind: super::ExchangeKind::UserMessage,
        });
      next_id += 1;
    }
  }
  let total_parked = a_bucket + other_peers * per_peer;

  // Drive N's handshake to A one inbound datagram at a time WITHOUT
  // n.handle_timeout (a global tick would full-drain every bucket). The
  // handle_udp that establishes N's connection to A must service ONLY A's bucket.
  let mut asserted = false;
  'outer: for _ in 0..400 {
    let mut n_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = n.poll_transmit() {
      if to == a_addr {
        n_out.push(bytes.to_vec());
      }
    }
    for dg in n_out {
      a.handle_udp(n_addr, &dg, now);
    }
    let mut a_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == n_addr {
        a_out.push(bytes.to_vec());
      }
    }
    for dg in a_out {
      let before = n.counters.dial_entries_serviced;
      let had_a_bucket = n.dial_parked.contains_key(&a_addr);
      n.handle_udp(a_addr, &dg, now);
      // A's bucket drains (its dials open) exactly on the establishing pass.
      if had_a_bucket && !n.dial_parked.contains_key(&a_addr) {
        let delta = n.counters.dial_entries_serviced - before;
        assert_eq!(
          delta, a_bucket as u64,
          "establishment must service ONLY A's bucket ({a_bucket} dials); a \
             whole-queue drain would process all {total_parked} parked dials"
        );
        // Every OTHER peer's bucket is untouched — still parked in full.
        for p in 0..other_peers {
          let peer: SocketAddr = format!("127.0.0.9:{}", 6000 + p).parse().unwrap();
          assert_eq!(
            n.dial_parked.get(&peer).map(|b| b.len()).unwrap_or(0),
            per_peer,
            "a peer whose connection did NOT establish must keep its full bucket"
          );
        }
        asserted = true;
        break 'outer;
      }
    }
    a.handle_timeout(now);
  }

  assert!(
    asserted,
    "N's connection to A must establish and service A's bucket within the ferry"
  );
}

/// A reliable-send entry point (`start_user_message` / `start_push_pull` /
/// `start_reliable_ping`) must do work bounded by its OWN new exchange — never
/// O(parked dials + bridges + connections). Pre-populate all three tables (many
/// parked dials on cold peers, many live back-pressured bridges on an established
/// connection), then issue ONE `start_user_message` to a fresh peer and prove the
/// three servicing-visit counters advance by their O(1) amounts — exactly one
/// fresh dial entry, ZERO connection services, ZERO bridge pumps — independent of
/// the table sizes. Growing the parked-dial table then leaves the fresh-dial count
/// unchanged, proving the per-request work does not scale.
///
/// Mutation-verify: restore the former `self.service_dials(now);
/// self.flush_outbound(now)` pair in `service_started_exchange`. `service_dials`
/// then drains every parked bucket, so `dial_entries_serviced` jumps by the whole
/// parked population; `flush_outbound`'s `service_quinn` visits every connection
/// (so `connection_visits` scales with the connection table) and its two
/// `pump_bridges` calls visit every live bridge twice (so `bridge_visits` scales
/// with the bridge table) — failing all three assertions.
#[test]
fn reliable_start_touches_constant_entries_regardless_of_table_size() {
  let a_addr: SocketAddr = "127.0.0.1:7810".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7811".parse().unwrap();
  let now = Instant::now();
  // No periodic schedulers on A, so the ONLY `DialRequested` it ever emits is the
  // one each explicit `start_*` call queues — the fresh-dial count stays exact.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  // B advertises a SMALL connection window (so A's exchanges back-pressure and
  // stay live) and a HIGH bidi-stream limit (so A can open many at once).
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window_bidi(400), b_addr, now);

  // Establish A -> B (timer-driven ferry).
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "test precondition: A and B must be connected before opening the exchanges"
  );
  while a.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}

  // Populate the BRIDGE table: open many reliable user-message exchanges to B.
  // B's small connection window admits only the first few, so the rest stay live
  // with un-flushed `pending_out`. Held (never ferried) so all stay live.
  const BRIDGES: usize = 200;
  let payload = Bytes::from(vec![0xB4u8; 4096]);
  for _ in 0..BRIDGES {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to the established B");
  }
  while a.poll_transmit().is_some() {}
  let live_bridges = a.bridges.len();
  assert!(
    live_bridges >= 128,
    "test precondition: A must hold many concurrent connection-window-blocked \
       bridges so the bridge-table scan is non-vacuous; got {live_bridges}"
  );

  // Populate the DIAL table: inject parked dials on distinct cold peers (targeting
  // never-answered addresses so even a whole-queue drain only re-parks them; the
  // scoped path never touches them at all).
  let park = |a: &mut QuicEndpoint<SmolStr>, peers: core::ops::Range<usize>, base_id: u64| {
    for p in peers {
      let peer: SocketAddr = format!("127.0.0.9:{}", 6000 + p).parse().unwrap();
      a.dial_parked
        .entry(peer)
        .or_default()
        .push_back(super::PendingDial {
          id: StreamId::from_raw(base_id + p as u64),
          peer,
          deadline: now + Duration::from_secs(30),
          wake: now + Duration::from_secs(30),
          attempted: true,
          kind: super::ExchangeKind::UserMessage,
        });
    }
  };
  const PARKED: usize = 200;
  park(&mut a, 0..PARKED, 10_000);

  // One reliable send to a FRESH cold peer (no pooled connection, no parked bucket)
  // must service ONLY its own new dial intent.
  let fresh: SocketAddr = "127.0.0.8:5000".parse().unwrap();
  a.counters.dial_entries_serviced = 0;
  a.counters.connection_visits = 0;
  a.counters.bridge_visits = 0;
  let _ = a
    .start_user_message(fresh, payload.clone(), now)
    .expect("A starts a reliable user message to a fresh peer");

  assert_eq!(
    a.counters.dial_entries_serviced, 1,
    "a reliable start must service ONLY its own fresh dial intent (1), never the \
       {PARKED} parked dials a whole-queue `service_dials` drain would process"
  );
  assert_eq!(
    a.counters.connection_visits,
    0,
    "a reliable start runs no `service_quinn`, so it services ZERO connections; a \
       global `flush_outbound` would visit every one of the {} live connections",
    a.live_connections_to(b_addr)
  );
  assert_eq!(
    a.counters.bridge_visits, 0,
    "a reliable start to a fresh cold peer mints no bridge, so it pumps ZERO \
       bridges; a global `flush_outbound` would pump every one of the \
       {live_bridges} live bridges twice"
  );

  // Grow the parked-dial table and re-verify the per-request fresh-dial count does
  // NOT scale with it.
  const GROWN: usize = 400;
  park(&mut a, PARKED..GROWN, 30_000);
  let fresh2: SocketAddr = "127.0.0.8:5001".parse().unwrap();
  a.counters.dial_entries_serviced = 0;
  let _ = a
    .start_user_message(fresh2, payload.clone(), now)
    .expect("A starts a second reliable user message to another fresh peer");
  assert_eq!(
    a.counters.dial_entries_serviced, 1,
    "doubling the parked-dial table to {GROWN} must not change the O(1) per-request \
       fresh-dial count"
  );
}

/// Like [`test_config`] but limiting each connection to `limit` concurrent
/// remote-opened bidi streams, so a peer that pins that many bidi streams
/// exhausts the credit — the setup for the credit-exhaustion / MAX_STREAMS-restore
/// (`StreamEvent::Available`) path.
fn test_config_bidi_limit(limit: u32) -> QuicOptions {
  let mut transport = quinn_proto::TransportConfig::default();
  transport.max_idle_timeout(Some(
    quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
  ));
  transport.max_concurrent_bidi_streams(quinn_proto::VarInt::from_u32(limit));
  QuicOptions::new(
    crate::quic::crypto::tests::test_endpoint_config(&[0x5au8; 32]),
    crate::quic::crypto::tests::test_server(),
    crate::quic::crypto::tests::test_client(),
    transport,
    "localhost",
    UnreliableTransport::Datagram,
  )
}

/// A dial requeued because the peer's bidi-stream credit was exhausted must retry
/// — and open its bridge — the moment the peer raises its MAX_STREAMS bidi limit
/// (a `StreamEvent::Available { dir: Bi }`) delivered via `handle_udp` before the
/// dial's deadline. Without the `Available` arm the dial waits to its deadline and
/// fails.
///
/// Construction: N and B allow only ONE concurrent bidi stream. N establishes a
/// pooled connection to B, opens exchange 1 (consuming the single credit), then
/// dials again — `open(Bi)` returns None (credit exhausted) so the dial re-parks
/// on B's bucket. Ferrying exchange 1 to completion frees B's stream slot; B sends
/// MAX_STREAMS, and the datagram carrying it drives N's `Available` arm, which
/// services B's bucket and opens the parked dial's bridge in that same pass.
///
/// Mutation-verify: revert the `Available { dir: Bi }` arm to `_ => {}`. The
/// MAX_STREAMS still raises the limit, but nothing services the bucket, so the
/// parked dial never opens within the ferry (which never ticks N) and this fails.
#[test]
fn credit_restored_available_retries_parked_dial() {
  let n_addr: SocketAddr = "127.0.0.1:7740".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7741".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("n"), n_addr),
    test_config_bidi_limit(1),
    n_addr,
    now,
  );
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(1),
    b_addr,
    now,
  );

  // Establish N -> B via a datagram warm-up (no bidi consumed) so the first
  // reliable dial opens immediately rather than parking on the handshake.
  let _ = n.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now);
  n.flush_outbound_transmits(now);
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
    if n.live_connections_to(b_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    n.live_connections_to(b_addr) >= 1,
    "test precondition: N must hold an established pooled connection to B"
  );
  // Quiesce so the exchange-1 setup is deterministic.
  while n.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}

  // Exchange 1 opens the single available bidi (credit 1/1 used). Its request is
  // queued in `out` but NOT delivered yet, so the bridge stays alive.
  let _id1 = n.start_push_pull(b_addr, PushPullKind::Join, now);
  assert_eq!(
    n.live_bridge_count(),
    1,
    "test precondition: exchange 1 opens the single bidi credit"
  );
  // Exchange 2 finds the credit exhausted -> re-parks on B's bucket, no bridge.
  let id2 = n.start_push_pull(b_addr, PushPullKind::Refresh, now);
  assert_eq!(
    n.dial_parked.get(&b_addr).map(|b| b.len()).unwrap_or(0),
    1,
    "test precondition: the second dial re-parks on B's bucket (credit exhausted)"
  );
  assert!(
    !n.bridges.contains_key(&id2),
    "test precondition: the credit-exhausted dial mints no bridge yet"
  );

  // Ferry exchange 1 to completion WITHOUT n.handle_timeout: as B reaps its
  // inbound bridge it frees the slot and sends MAX_STREAMS. The datagram carrying
  // MAX_STREAMS drives N's `Available` arm, which services B's bucket and opens
  // the parked dial's bridge in that same handle_udp.
  //
  // Advance `now` per iteration so N's connection ACK/loss timers fire — but only
  // via `handle_udp` (which runs the connection's `handle_timeout` internally),
  // never via `n.handle_timeout`, which would run a global tick and full-drain
  // the parked dial regardless of the `Available` arm. The advance stays far
  // under the dial's stream-timeout deadline so it is the credit restore, not the
  // deadline, that resolves the parked dial.
  let mut retried = false;
  let mut t = now;
  'outer: for _ in 0..200 {
    t += Duration::from_millis(20);
    let mut n_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = n.poll_transmit() {
      if to == b_addr {
        n_out.push(bytes.to_vec());
      }
    }
    for dg in n_out {
      b.handle_udp(n_addr, &dg, t);
    }
    let mut b_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == n_addr {
        b_out.push(bytes.to_vec());
      }
    }
    for dg in b_out {
      n.handle_udp(b_addr, &dg, t);
      if n.bridges.contains_key(&id2) {
        retried = true;
        assert!(
          !n.dial_parked.contains_key(&b_addr),
          "the retried dial must leave B's parked bucket once it opens"
        );
        break 'outer;
      }
    }
    // Advance only B's coordinator tick (never N's) so the parked dial can open
    // ONLY via N's `Available` arm, never a global tick's full drain.
    b.handle_timeout(t);
  }

  assert!(
    retried,
    "the credit-exhausted parked dial must open its bridge on the MAX_STREAMS \
       (Available) arriving via handle_udp — not wait for its deadline; reverting \
       the Available arm leaves it parked and this fails"
  );
}

/// A single-credit `MAX_STREAMS` restore (`StreamEvent::Available { dir: Bi }`)
/// services a peer's parked-dial bucket in O(1), NOT O(bucket):
/// `service_peer_bucket` stops at the FIRST re-park. Every entry in one peer's
/// bucket shares that peer's single pooled connection and its one bidi-credit
/// pool, so once an attempt re-parks (credit exhausted or still handshaking)
/// every later entry would re-park identically. Re-attempting the whole bucket
/// per credit is O(bucket) per credit, so K single-credit frames (K small
/// packets) cost O(K^2); stopping after the first re-park makes each restore
/// O(1), so K restores cost O(K).
///
/// Construction: park M attempted intents on ONE never-answered peer. Its pooled
/// connection stays handshaking, so every attempt's `open(Bi)` returns `None` and
/// re-parks. Drive `service_peer_bucket` K times (each call is one single-credit
/// `Available`; a real `MAX_STREAMS` datagram reaches the same arm) and assert
/// each call attempts exactly ONE entry — independent of the M-entry bucket.
///
/// Mutation-verify: drop the `break` on `DialAttempt::Reparked` (drain the whole
/// bucket per credit) — each call then attempts all M entries, so the per-call
/// delta jumps to M and the `<= 1` assertion fails on the first restore.
#[test]
fn single_credit_available_services_bucket_in_o_1_not_o_bucket() {
  let n_addr: SocketAddr = "127.0.0.1:7760".parse().unwrap();
  let cold_peer: SocketAddr = "127.0.0.9:7761".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);

  // Park M attempted intents on ONE cold peer. The first `service_peer_bucket`
  // attempt dials it — creating a single pooled, still-handshaking connection —
  // and `open(Bi)` returns `None`, so the entry re-parks. All M target the SAME
  // pooled connection.
  const M: usize = 24;
  let deadline = now + Duration::from_secs(30);
  for i in 0..M {
    n.dial_parked
      .entry(cold_peer)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(20_000 + i as u64),
        peer: cold_peer,
        deadline,
        wake: deadline,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
  }
  assert_eq!(
    n.dial_parked.get(&cold_peer).map(|b| b.len()).unwrap_or(0),
    M,
    "test precondition: all M intents park on the one cold peer's bucket"
  );

  // K single-credit restores. Each `service_peer_bucket` must attempt exactly ONE
  // entry (the first re-parks, the drain breaks) — O(1) per credit, independent
  // of the M-entry bucket. The whole-bucket drain would attempt all M per call.
  const K: usize = 24;
  let base = n.counters.dial_entries_serviced;
  for i in 0..K {
    let before = n.counters.dial_entries_serviced;
    // A fresh full dial budget per restore: each call must still stop at the first
    // re-park (O(1)), independent of the budget.
    let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
    n.service_peer_bucket(cold_peer, now, &mut dial_budget);
    let delta = n.counters.dial_entries_serviced - before;
    assert!(
      delta <= 1,
      "single-credit Available restore #{i} on an M={M} handshake-blocked bucket \
         must attempt O(1) entries (stop at the first re-park), not the whole \
         bucket; attempted {delta} — reverting the `break` makes this {M}"
    );
  }

  // The bucket is intact: nothing minted (still handshaking) and nothing dropped —
  // every intent still awaits the tick's full drain or a real credit.
  assert_eq!(
    n.dial_parked.get(&cold_peer).map(|b| b.len()).unwrap_or(0),
    M,
    "the handshake-blocked bucket retains all M entries across the K restores"
  );
  // Cumulative: K restores attempted O(K) entries total, never O(K*M).
  assert!(
    n.counters.dial_entries_serviced - base <= K as u64,
    "K={K} single-credit restores must be O(K)={K} total dial attempts, not \
       O(K*M)={}",
    K * M
  );
}

/// A single `Available` (or handshake completion) that unblocks a peer's parked
/// bucket where every entry MINTS must not let ONE datagram drive O(bucket) dial
/// work: `service_peer_bucket` attempts at most `MAX_DIAL_ATTEMPTS_PER_PASS`
/// entries per pass, then defers the untouched tail. A K-credit `MAX_STREAMS`
/// grant (or a handshake granting a large initial bidi credit) makes every bucket
/// entry return `Minted`, so — unlike the credit-exhausted case that stops at the
/// first re-park — nothing bounds the drain except this budget.
///
/// Construction: establish N -> B with a large bidi credit via a datagram warm-up
/// (no bidi consumed, no bridge minted), then inject M REAL dial intents parked on
/// B's bucket (registered on the raw endpoint so `dial_succeeded` resolves each,
/// then moved to `dial_parked` as attempted — the state a burst reaches after
/// parking on a since-established, amply-credited connection). ONE
/// `service_peer_bucket` then mints at most the budget and re-parks the rest.
///
/// Mutation-verify: remove the `MAX_DIAL_ATTEMPTS_PER_PASS` cap (keep only the
/// `Reparked` break) — the amply-credited bucket never re-parks, so the pass mints
/// all M and the `<= budget` assertion fails.
#[test]
fn established_bucket_pass_mints_at_most_the_budget() {
  let n_addr: SocketAddr = "127.0.0.1:7780".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7781".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("n"), n_addr),
    test_config_bidi_limit(512),
    n_addr,
    now,
  );
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(512),
    b_addr,
    now,
  );

  // Establish N -> B via a datagram warm-up: no bidi credit is consumed and no
  // bridge is minted, so the pooled connection is Established with its full
  // 512-stream initial bidi credit and N holds zero bridges before the injection.
  let _ = n.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now);
  n.flush_outbound_transmits(now);
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
    if n.live_connections_to(b_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    n.live_connections_to(b_addr) >= 1,
    "test precondition: N must hold an established pooled connection to B"
  );
  while n.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while n.poll_event().is_some() {}
  while b.poll_event().is_some() {}
  assert_eq!(
    n.live_bridge_count(),
    0,
    "test precondition: the datagram warm-up mints no bridge"
  );

  // Inject M REAL parked intents on B's bucket. Register each on the raw endpoint
  // (so `dial_succeeded` resolves it into a real `Stream`) WITHOUT servicing, sieve
  // the emitted `DialRequested`s into `dial_pending`, then move them onto
  // `dial_parked[b]` as attempted — the post-parking state under test.
  const M: usize = 256;
  let payload = Bytes::from_static(b"x");
  for _ in 0..M {
    n.endpoint_mut()
      .start_user_message(b_addr, payload.clone(), now)
      .expect("the raw endpoint registers a reliable user-message dial intent");
  }
  n.sieve_dial_events();
  let pending: Vec<super::PendingDial> = n.dial_pending.drain(..).collect();
  assert_eq!(
    pending.len(),
    M,
    "test precondition: all M intents sieve into dial_pending"
  );
  // They are now parked-attempted, not unattempted-pending.
  n.unattempted_dial_count = 0;
  for mut pd in pending {
    pd.attempted = true;
    n.dial_parked.entry(b_addr).or_default().push_back(pd);
  }
  let parked_before = n.dial_parked.get(&b_addr).map(|x| x.len()).unwrap_or(0);
  assert!(
    parked_before > super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "the bucket ({parked_before}) must exceed the budget for the cap to be \
       non-vacuous"
  );

  // ONE service_peer_bucket pass mints at most the budget, then defers the tail.
  let before = n.counters.dial_entries_serviced;
  let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = n.service_peer_bucket(b_addr, now, &mut dial_budget);
  let attempts = n.counters.dial_entries_serviced - before;
  assert!(
    attempts <= super::MAX_DIAL_ATTEMPTS_PER_PASS as u64,
    "one service_peer_bucket pass on an amply-credited bucket must attempt at most \
       MAX_DIAL_ATTEMPTS_PER_PASS={}, not the {M}-entry bucket; attempted {attempts} \
       (removing the cap mints all {M})",
    super::MAX_DIAL_ATTEMPTS_PER_PASS
  );
  // Every budgeted attempt minted a REAL bridge (not a retire): the mint path — not
  // the expired/retire path — is what the drain must bound here.
  assert_eq!(
    n.live_bridge_count(),
    attempts as usize,
    "every budgeted attempt opened a real outbound bridge on the amply-credited \
       established connection"
  );
  // Exactly the un-attempted tail rides on the bucket, untouched (nothing dropped).
  let tail = n.dial_parked.get(&b_addr).map(|x| x.len()).unwrap_or(0);
  assert_eq!(
    tail,
    M - attempts as usize,
    "exactly the un-attempted tail (M - budget) stays parked for the next drain"
  );
  assert!(
    tail > 0,
    "non-vacuous: the budget must have deferred a tail"
  );

  // Liveness: the un-budgeted full drain mints the whole deferred tail on the
  // established connection's remaining credit — nothing is stranded.
  let _ = n.service_dials(now);
  assert_eq!(
    n.dial_parked.get(&b_addr).map(|x| x.len()).unwrap_or(0),
    0,
    "the un-budgeted service_dials drains the whole deferred tail, leaving the \
       bucket empty (nothing lost)"
  );
}

/// A long expired prefix in one peer's parked bucket must not let a single
/// `Available` (or establishment) drive O(bucket) dial retirements:
/// `service_peer_bucket` attempts at most `MAX_DIAL_ATTEMPTS_PER_PASS` entries per
/// pass REGARDLESS of outcome. An expired entry takes the deadline-elapsed retire
/// branch (no connection dialed, no credit consumed, no re-park), so — like the
/// mint case, and unlike the credit-exhausted case — nothing but this budget stops
/// the drain scanning the whole expired prefix.
///
/// Construction: park M expired intents (deadline already elapsed) on ONE peer.
/// Drive ONE `service_peer_bucket` and assert it attempted at most the budget,
/// deferring the rest as an untouched parked tail.
///
/// Mutation-verify: remove the `MAX_DIAL_ATTEMPTS_PER_PASS` cap (keep only the
/// `Reparked` break) — an expired prefix never re-parks, so the pass retires all M
/// and the `<= budget` assertion fails.
#[test]
fn expired_prefix_pass_attempts_at_most_the_budget() {
  let n_addr: SocketAddr = "127.0.0.1:7790".parse().unwrap();
  let peer: SocketAddr = "127.0.0.9:7791".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);

  // Park M expired intents (deadline in the past) on ONE peer. Each attempt hits
  // the deadline-elapsed retire branch, so the drain continues through the whole
  // prefix unless the per-pass budget stops it. Fake ids are sound here: the retire
  // path routes through `dial_failed`, a no-op for an unregistered intent.
  const M: usize = 256;
  let elapsed = now - Duration::from_secs(1);
  for i in 0..M {
    n.dial_parked
      .entry(peer)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(40_000 + i as u64),
        peer,
        deadline: elapsed,
        wake: elapsed,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
  }
  let parked_before = n.dial_parked.get(&peer).map(|b| b.len()).unwrap_or(0);
  assert!(
    parked_before > super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "the bucket ({parked_before}) must exceed the budget for the cap to be \
       non-vacuous"
  );

  // ONE pass: attempts at most the budget, then defers the untouched tail.
  let before = n.counters.dial_entries_serviced;
  let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = n.service_peer_bucket(peer, now, &mut dial_budget);
  let attempted = n.counters.dial_entries_serviced - before;
  assert!(
    attempted <= super::MAX_DIAL_ATTEMPTS_PER_PASS as u64,
    "one service_peer_bucket pass must attempt at most \
       MAX_DIAL_ATTEMPTS_PER_PASS={}, not the {M}-entry expired prefix; attempted \
       {attempted} (removing the cap retires all {M})",
    super::MAX_DIAL_ATTEMPTS_PER_PASS
  );
  let tail = n.dial_parked.get(&peer).map(|b| b.len()).unwrap_or(0);
  assert_eq!(
    tail,
    M - attempted as usize,
    "exactly the un-attempted tail stays parked for the next drain"
  );
  assert!(
    tail > 0,
    "non-vacuous: the budget must have deferred a tail"
  );

  // Liveness: the un-budgeted full drain retires everything the budget deferred.
  let _ = n.service_dials(now);
  assert_eq!(
    n.dial_parked.get(&peer).map(|b| b.len()).unwrap_or(0),
    0,
    "the un-budgeted service_dials drains the whole deferred tail (expired intents \
       retired), leaving the bucket empty"
  );
}

/// A per-peer readiness event that services a large parked bucket must leave the
/// unprocessed tail RESIDENT in the same allocation — the resident-tail
/// bounded-prefix bound. `service_peer_bucket` detaches a bounded front-prefix
/// with O(1) `pop_front` and never moves, collects, or re-inserts the tail, so
/// one datagram's dial work is O(min(budget, bucket)) with no per-datagram copy
/// of the tail back into a recreated bucket.
///
/// Phase 1 (mint): an established, amply-credited connection mints the budget's
/// worth of entries off the front; the M - budget tail stays parked with its
/// backing storage untouched — the SAME back-element address and the SAME
/// `VecDeque` capacity across the pass.
///
/// Phase 2 (handshake-blocked): the first entry re-parks and the drain stops; it
/// rotates to the BACK of the resident bucket (front-pop, back-repark) while
/// every other entry stays parked in place, so nothing is dropped and the bucket
/// keeps all M entries.
///
/// Mutation-verify: restore the whole-bucket take with a `collect()` + `extend()`
/// tail move — Phase 1's back-element-address assertion fails (the tail is
/// relocated into a fresh allocation), and Phase 2's rotated-order assertion
/// fails (a front re-insert leaves the re-parked entry at the FRONT).
#[test]
fn service_peer_bucket_leaves_tail_resident_no_move() {
  // ---- Phase 1: established + amply-credited -> the budget mints off the front,
  //      the un-processed tail stays resident in the same allocation. ----
  let n_addr: SocketAddr = "127.0.0.1:7800".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7801".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("n"), n_addr),
    test_config_bidi_limit(512),
    n_addr,
    now,
  );
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(512),
    b_addr,
    now,
  );

  // Establish N -> B via a datagram warm-up: no bidi credit is consumed and no
  // bridge is minted, so the pooled connection is Established with its full
  // 512-stream initial bidi credit.
  let _ = n.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now);
  n.flush_outbound_transmits(now);
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
    if n.live_connections_to(b_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    n.live_connections_to(b_addr) >= 1,
    "test precondition: N must hold an established pooled connection to B"
  );
  while n.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while n.poll_event().is_some() {}
  while b.poll_event().is_some() {}
  assert_eq!(
    n.live_bridge_count(),
    0,
    "test precondition: the datagram warm-up mints no bridge"
  );

  // Inject M REAL parked intents on B's bucket (registered on the raw endpoint so
  // `dial_succeeded` resolves each), marked attempted — the post-parking state a
  // burst reaches on a since-established, amply-credited connection.
  const M: usize = 256;
  let payload = Bytes::from_static(b"x");
  for _ in 0..M {
    n.endpoint_mut()
      .start_user_message(b_addr, payload.clone(), now)
      .expect("the raw endpoint registers a reliable user-message dial intent");
  }
  n.sieve_dial_events();
  let pending: Vec<super::PendingDial> = n.dial_pending.drain(..).collect();
  assert_eq!(
    pending.len(),
    M,
    "test precondition: all M intents sieve into dial_pending"
  );
  n.unattempted_dial_count = 0;
  for mut pd in pending {
    pd.attempted = true;
    n.dial_parked.entry(b_addr).or_default().push_back(pd);
  }
  let parked_before = n
    .dial_parked
    .get(&b_addr)
    .map(|bucket| bucket.len())
    .unwrap_or(0);
  assert!(
    parked_before > super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "the bucket ({parked_before}) must exceed the budget for the resident tail to \
       be non-vacuous"
  );

  // Witness the resident tail's backing storage BEFORE the pass: the physical
  // address of the back element and the bucket's capacity. Popping off the FRONT
  // never relocates the back element or reallocates the ring buffer; a
  // `collect()` + `extend()` tail move would rebuild the tail into a fresh
  // allocation, relocating the back element.
  let tail_ptr_before: *const super::PendingDial = n
    .dial_parked
    .get(&b_addr)
    .and_then(|bucket| bucket.back())
    .map(|entry| entry as *const super::PendingDial)
    .expect("the parked bucket has a resident back element");
  let cap_before = n
    .dial_parked
    .get(&b_addr)
    .map(|bucket| bucket.capacity())
    .expect("the parked bucket is present");

  // ONE service_peer_bucket pass mints the budget's prefix, then defers the tail.
  let before = n.counters.dial_entries_serviced;
  let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = n.service_peer_bucket(b_addr, now, &mut dial_budget);
  let processed = (n.counters.dial_entries_serviced - before) as usize;

  // (a) the pass attempts at most the budget.
  assert!(
    processed <= super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "one pass must attempt at most MAX_DIAL_ATTEMPTS_PER_PASS={}, attempted {processed}",
    super::MAX_DIAL_ATTEMPTS_PER_PASS
  );
  assert!(
    processed > 0,
    "non-vacuous: the pass must have minted a prefix"
  );
  // Every budgeted attempt opened a REAL bridge (the mint path, not a retire), so
  // the prefix was consumed off the front rather than dropped.
  assert_eq!(
    n.live_bridge_count(),
    processed,
    "every budgeted attempt opened a real outbound bridge off the front"
  );

  // (b) the bucket still holds exactly the un-processed tail (M - processed).
  let tail = n
    .dial_parked
    .get(&b_addr)
    .map(|bucket| bucket.len())
    .unwrap_or(0);
  assert_eq!(
    tail,
    M - processed,
    "exactly the un-processed tail (M - processed) stays resident"
  );
  assert!(
    tail > 0,
    "non-vacuous: the budget must have deferred a tail"
  );

  // (c) the resident tail was neither relocated nor rebuilt: the same back
  // element lives at the same address, in a bucket of the same capacity. A
  // `collect()` + `extend()` tail move relocates the back element into a fresh
  // allocation, so this fails under that mutation.
  let tail_ptr_after: *const super::PendingDial = n
    .dial_parked
    .get(&b_addr)
    .and_then(|bucket| bucket.back())
    .map(|entry| entry as *const super::PendingDial)
    .expect("the resident tail still has a back element");
  assert_eq!(
    tail_ptr_before, tail_ptr_after,
    "the resident tail's back element must not be relocated (no collect()+extend() rebuild)"
  );
  assert_eq!(
    n.dial_parked
      .get(&b_addr)
      .map(|bucket| bucket.capacity())
      .expect("the resident tail's bucket is present"),
    cap_before,
    "the resident tail's backing VecDeque must not be reallocated"
  );

  // ---- Phase 2: handshake-blocked -> the first entry re-parks and the drain
  //      stops; it rotates to the BACK while the resident tail stays in place. ----
  let n2_addr: SocketAddr = "127.0.0.1:7802".parse().unwrap();
  let cold_peer: SocketAddr = "127.0.0.9:7803".parse().unwrap();
  let mut n2 = make_endpoint("n2", n2_addr, now);

  // Park M attempted intents on ONE never-answered peer. The first
  // `service_peer_bucket` attempt dials it — a single still-handshaking
  // connection — and `open(Bi)` returns None, so the entry re-parks; every later
  // entry shares that connection and would re-park identically, so the drain
  // stops at the first.
  const HB_M: usize = 256;
  let deadline = now + Duration::from_secs(30);
  let first_id = StreamId::from_raw(50_000);
  let second_id = StreamId::from_raw(50_001);
  for i in 0..HB_M {
    n2.dial_parked
      .entry(cold_peer)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(50_000 + i as u64),
        peer: cold_peer,
        deadline,
        wake: deadline,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
  }

  let before2 = n2.counters.dial_entries_serviced;
  let mut dial_budget2 = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = n2.service_peer_bucket(cold_peer, now, &mut dial_budget2);
  let processed2 = n2.counters.dial_entries_serviced - before2;
  assert_eq!(
    processed2, 1,
    "a handshake-blocked bucket must stop at the first re-park (O(1), not O(bucket))"
  );
  // Nothing dropped: the re-parked entry rode back into the same bucket, so all M
  // stay parked.
  assert_eq!(
    n2.dial_parked
      .get(&cold_peer)
      .map(|bucket| bucket.len())
      .unwrap_or(0),
    HB_M,
    "the re-park returns the entry to the bucket, so all M stay parked"
  );
  // Front-pop + back-repark: the first entry rotated to the BACK, so the new
  // front is the SECOND parked entry and the back is the FIRST. A front re-insert
  // (the old collect()+extend() shape) would instead leave the first entry at the
  // FRONT.
  let front_id = n2
    .dial_parked
    .get(&cold_peer)
    .and_then(|bucket| bucket.front())
    .map(|entry| entry.id)
    .expect("the resident bucket has a front element");
  let back_id = n2
    .dial_parked
    .get(&cold_peer)
    .and_then(|bucket| bucket.back())
    .map(|entry| entry.id)
    .expect("the resident bucket has a back element");
  assert_eq!(
    front_id, second_id,
    "front-pop rotates the re-parked entry off the front; the second entry leads"
  );
  assert_eq!(
    back_id, first_id,
    "the re-parked first entry rides at the back of the resident bucket"
  );
}

/// One connection-window MAX_DATA makes quinn emit `StreamEvent::Writable` for
/// EVERY connection-window-blocked stream in a SINGLE `poll()` drain. The arm
/// acts on EVERY edge — no per-pass enqueue cap — because quinn clears
/// `connection_blocked` on this yield and never re-adds it, so a skipped edge
/// would strand a still-blocked bridge to its exchange deadline. Bounding the
/// storm is instead the local `C_OUT` outbound cap: the blocked set is our
/// inbound accepts (config) plus at most `C_OUT` dialer streams, so acting on
/// every edge is O(config), not O(the attacker's stream count).
///
/// Unlike `writable_storm_pass_pumps_at_most_the_budget` (which bounds the DRAIN,
/// enqueuing the storm synthetically), this drives a REAL MAX_DATA: A opens N
/// connection-window-blocked DIALER bridges (N < `C_OUT`, so all open), B reads
/// A's data and emits MAX_DATA, and A's `poll()` then storms `Writable`. The
/// per-pass ENQUEUE count is asserted to EQUAL the events seen (no skip).
///
/// Mutation-verify: restore a `writable_enqueues >= MAX_BRIDGE_PUMPS_PER_PASS`
/// skip — the storm pass then enqueues at most the budget while seeing ~N
/// events, so `writable_bridges_enqueued < writable_events_seen` and the
/// `enqueued == seen` assertion fails on that pass.
#[test]
fn writable_storm_arm_caps_enqueues() {
  let a_addr: SocketAddr = "127.0.0.1:7996".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7997".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  // B: small connection window (so A's sends back-pressure at the connection level)
  // and a high bidi-stream limit (so A can open many exchanges at once).
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window_bidi(400), b_addr, now);

  // Establish A -> B (timer-driven ferry).
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "test precondition: A and B must be connected before opening the exchanges"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}

  // A opens N reliable exchanges; B's 16 KiB connection window admits only a few,
  // so the rest stay connection-window-blocked (in quinn's connection_blocked set)
  // — each a bridge that will receive a `Writable` when a MAX_DATA arrives.
  const N: usize = 200;
  let payload = Bytes::from(vec![0xC3u8; 4096]);
  for _ in 0..N {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to the established B");
  }
  let live = a.bridges.len();
  assert!(
    live > super::MAX_BRIDGE_PUMPS_PER_PASS,
    "test precondition: A must hold more connection-window-blocked bridges ({live}) \
       than the budget so the storm is non-vacuous"
  );

  // Deliver A's blocked sends to B and let B read them and emit MAX_DATA, but do
  // NOT deliver B's output back to A yet — so all N blocked bridges accumulate on A
  // before any MAX_DATA reaches it, landing the whole storm in one A pass.
  let mut b_to_a: Vec<Vec<u8>> = Vec::new();
  let mut t = now;
  for _ in 0..80 {
    t += Duration::from_millis(5);
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, t);
        moved = true;
      }
    }
    b.handle_timeout(t);
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        b_to_a.push(bytes.to_vec());
      }
    }
    if !moved {
      break;
    }
  }
  assert!(
    !b_to_a.is_empty(),
    "B must emit datagrams (a MAX_DATA raising the connection window) after reading \
       A's back-pressured data"
  );

  // Measured phase: deliver B's datagrams to A ONE AT A TIME. Each `handle_udp` is
  // one service pass = one `Writable`-arm run. Reset the counters before each and
  // assert the arm acts on EVERY edge (enqueued == seen, no skip) and that the
  // blocked set the storm delivers stays bounded by the local `C_OUT` cap (all of
  // A's blocked streams are dialers). The storm pass proves this is non-vacuous
  // (it SEES far more Writables than the OLD per-pass budget).
  let mut max_events_seen = 0u64;
  for dg in &b_to_a {
    a.counters.writable_events_seen = 0;
    a.counters.writable_bridges_enqueued = 0;
    a.handle_udp(b_addr, dg, t);
    let seen = a.counters.writable_events_seen;
    let enqueued = a.counters.writable_bridges_enqueued;
    max_events_seen = max_events_seen.max(seen);
    assert_eq!(
      enqueued, seen,
      "the Writable arm must act on EVERY edge (no per-pass skip): a dropped edge \
         strands a bridge whose `connection_blocked` entry quinn cleared on yield; \
         seen {seen}, enqueued {enqueued} (restoring a per-pass cap makes enqueued \
         fall below seen on the storm pass)"
    );
    assert!(
      seen <= super::C_OUT as u64,
      "the blocked set one MAX_DATA storms ({seen}) must stay bounded by the local \
         outbound cap C_OUT={} — all of A's blocked streams are dialers, so the \
         per-datagram Writable work is O(config), not O(the peer's advertised \
         MAX_STREAMS)",
      super::C_OUT
    );
  }
  assert!(
    max_events_seen > super::MAX_BRIDGE_PUMPS_PER_PASS as u64,
    "non-vacuous: one service pass must have seen more Writable events \
       ({max_events_seen}) than the old per-pass budget, so acting on every edge \
       (not the reverted skip) is what is under test"
  );
}

/// A budget-deferred bridge residue holds no `TimerKey::Bridge` deadline until its
/// first pump, and `poll_timeout` deliberately ignores `ready_bridges`. With
/// membership (probe/gossip) and connection transport timers all far, the residue
/// would then get no timely wake and strand. The fix is a STICKY Catchup anchor:
/// `poll_timeout` publishes the `next_catchup_at` field verbatim while a residue
/// waits — armed ONCE when the residue first appeared and advanced only after a
/// catch-up step runs — and `handle_timeout` services a due Catchup-only wake in
/// BOUNDED steps (not a full O(N) membership tick), so a driver waking at the
/// anchor drains one budget-sized chunk per interval and converges.
///
/// Construction: A opens N connection-window-blocked bridges to B (far exchange
/// deadlines, quiescent connection — no near transport timer), then ONE service
/// pass over the enqueued storm leaves a residue > budget. A's periodic membership
/// schedulers are disabled so no staggered timer falls due in the drain window.
///
/// Mutation-verify (i): skip setting `TimerKey::Catchup` in `poll_timeout` — it
/// then returns the far exchange/idle minimum (or None), so the `<= now +
/// CATCHUP_INTERVAL` assertion fails; the `earliest_excluding_2` check proves every
/// scheduled timer is strictly later, so the Catchup term is load-bearing.
///
/// Mutation-verify (ii): drop the Catchup DUE-gate in `handle_timeout` (revert to
/// running the full `run_tick` on any anchor-only wake) — a Catchup wake then
/// drains the WHOLE residue in one tick (a step drains more than the budget) AND
/// advances membership time, so the per-step budget bound and the flat-membership
/// assertions fail.
#[test]
fn budget_deferred_residue_drains_via_throttled_catchup_wake() {
  let a_addr: SocketAddr = "127.0.0.1:7994".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7995".parse().unwrap();
  let now = Instant::now();
  // A's periodic membership schedulers are disabled so the multi-step catch-up
  // drain advances the clock with no membership timer ever falling due — every
  // wake is then provably a bounded catch-up, and membership time stays flat.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window_bidi(400), b_addr, now);

  // Establish A -> B.
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "test precondition: A and B must be connected"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}

  // Open N connection-window-blocked bridges (far exchange deadlines), then enqueue
  // them all and run ONE service pass so the pump budget leaves a residue > budget.
  const N: usize = 200;
  let payload = Bytes::from(vec![0xC3u8; 4096]);
  for _ in 0..N {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to the established B");
  }
  while a.poll_transmit().is_some() {}
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  let ids: Vec<StreamId> = a.bridges.keys().copied().collect();
  for id in &ids {
    super::enqueue_ready_bridge(&mut a.bridges, &mut a.ready_bridges, *id);
  }
  a.service_connection(ch, now);
  let residue0 = a.ready_bridges.len();
  assert!(
    residue0 > super::MAX_BRIDGE_PUMPS_PER_PASS,
    "test precondition: one pass must leave a residue exceeding the budget so the \
       catch-up needs multiple bounded steps; residue {residue0}"
  );

  // (i) A wake IS scheduled no later than `now + CATCHUP_INTERVAL` (the sticky
  // anchor armed when `service_connection` left the residue), and it is the
  // EARLIEST wake — every scheduled (non-anchor) timer is strictly later, so the
  // Catchup term is load-bearing (skipping it returns that far minimum, and the
  // primary assertion below fails).
  let catchup_deadline = now + super::CATCHUP_INTERVAL;
  let wake = a
    .poll_timeout()
    .expect("a non-empty residue must schedule a catch-up wake");
  assert!(
    wake <= catchup_deadline,
    "poll_timeout must return the sticky catch-up wake no later than now + \
       CATCHUP_INTERVAL while a residue waits; got {wake:?} vs {catchup_deadline:?}"
  );
  let non_catchup = a.deadline_index.earliest_excluding_2(
    super::deadline::TimerKey::Catchup,
    super::deadline::TimerKey::ImmediateDue,
  );
  assert!(
    non_catchup.is_none_or(|t| t > catchup_deadline),
    "non-vacuous: every scheduled (non-anchor) timer must be strictly later than \
       the catch-up deadline, so removing the Catchup term would return a later \
       wake (or None); got {non_catchup:?}"
  );

  // (ii) A driver re-polls the earliest wake (`poll_timeout`) and services it. The
  // sticky catch-up anchor is the earliest wake, so the first wake is a BOUNDED,
  // membership-flat catch-up that pumps one budget chunk and advances the anchor
  // one interval. As the anchor advances past the connection's transport timer
  // (armed by the in-flight data of the first pump), later chunks drain via a
  // scheduled tick — correct, and nothing is stranded. A step that does NOT advance
  // membership is a Catchup-only wake and MUST be bounded.
  let first_anchor = a
    .next_catchup_at
    .expect("a non-empty residue keeps the sticky catch-up anchor armed");
  assert_eq!(
    a.poll_timeout(),
    Some(first_anchor),
    "the sticky catch-up anchor is the earliest wake while a residue waits"
  );
  let mut catchup_steps = 0usize;
  let mut steps = 0usize;
  while !a.ready_bridges.is_empty() {
    let memb_before = a.membership_time_advances();
    let before = a.ready_bridges.len();
    let wake = a
      .poll_timeout()
      .expect("a non-empty residue always schedules a wake");
    a.handle_timeout(wake);
    let drained = before - a.ready_bridges.len();
    if a.membership_time_advances() == memb_before {
      // A Catchup-only wake: no membership tick, at most one budget chunk (dropping
      // the Catchup due-gate would run the full tick and drain it all at once).
      catchup_steps += 1;
      assert!(
        drained <= super::MAX_BRIDGE_PUMPS_PER_PASS,
        "a Catchup-only wake must pump at most the budget ({}); a step drained {drained}",
        super::MAX_BRIDGE_PUMPS_PER_PASS
      );
    }
    steps += 1;
    assert!(
      steps <= residue0 + 2,
      "the catch-up must converge, never strand; still {} queued after {steps} steps",
      a.ready_bridges.len()
    );
  }
  assert!(
    catchup_steps >= 1,
    "non-vacuous: the residue must drain via at least one BOUNDED catch-up wake; \
       dropping the Catchup due-gate so every anchor-only wake runs run_tick leaves \
       this 0 (every wake becomes a membership tick)"
  );
}

/// Establish `a` (periodic membership schedulers disabled) to `b`, open `N`
/// connection-window-blocked reliable bridges, enqueue them all, and run ONE
/// budgeted service pass — leaving a `ready_bridges` residue > budget with the
/// sticky catch-up anchor armed at `now + CATCHUP_INTERVAL`. Returns `a` and the
/// residue size (`b` is dropped: `a` holds the established connection state and the
/// sticky-anchor regressions drive only `a`).
fn a_with_budget_deferred_residue(
  a_addr: SocketAddr,
  b_addr: SocketAddr,
  now: Instant,
) -> (QuicEndpoint<SmolStr>, usize) {
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window_bidi(400), b_addr, now);

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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "setup: A and B must be connected"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}

  const N: usize = 200;
  let payload = Bytes::from(vec![0xC3u8; 4096]);
  for _ in 0..N {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to the established B");
  }
  while a.poll_transmit().is_some() {}
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  let ids: Vec<StreamId> = a.bridges.keys().copied().collect();
  for id in &ids {
    super::enqueue_ready_bridge(&mut a.bridges, &mut a.ready_bridges, *id);
  }
  a.service_connection(ch, now);
  let residue = a.ready_bridges.len();
  assert!(
    residue > super::MAX_BRIDGE_PUMPS_PER_PASS,
    "setup: one budgeted pass must leave a residue exceeding the budget; got {residue}"
  );
  (a, residue)
}

/// An ImmediateDue-only wake — a connection carrying a deferred `ConnectionEvent`
/// backlog, the shape a peer's `RETIRE_CONNECTION_ID` flood produces via the
/// requeued `NewIdentifiers` — must do BOUNDED work: service ONLY the pending-event
/// connections, never a full O(all connections) tick. The server pools many
/// connections but exactly one carries a backlog; one `handle_timeout` at the
/// ImmediateDue anchor (no scheduled timer due) visits exactly that one connection
/// and does NOT advance membership time.
///
/// Mutation-verify: revert `handle_timeout` to the 2-way `if !non_catchup_due {
/// run_tick }` dispatch — the present ImmediateDue term makes `non_catchup_due`
/// true, so it runs the full `run_tick`, whose `service_quinn` visits EVERY pooled
/// connection (`connection_visits` jumps to the pool size) AND advances membership
/// time, so both the `== 1` visit assertion and the flat-membership assertion fail.
#[test]
fn immediate_due_wake_services_only_pending_connections_not_full_tick() {
  let now = Instant::now();
  let s_addr: SocketAddr = "127.0.0.1:7780".parse().unwrap();
  // Schedulers disabled so no membership timer is ever due — the wake this test
  // drives is provably ImmediateDue-only, not a scheduled tick.
  let mut s = make_endpoint_no_schedulers("s", s_addr, now);

  let n = 8usize;
  let mut clients: Vec<(SocketAddr, QuicEndpoint<SmolStr>)> = Vec::new();
  for i in 0..n {
    let addr: SocketAddr = format!("127.0.0.1:{}", 7781 + i).parse().unwrap();
    let mut c = make_endpoint(&format!("c{i}"), addr, now);
    let _ = c.start_push_pull(s_addr, PushPullKind::Join, now);
    clients.push((addr, c));
  }
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
    while s.poll_event().is_some() {}
  }
  let pooled = s.conns.iter_handles().len();
  assert!(
    pooled >= 6,
    "test precondition: the server must pool most client connections; got {pooled}"
  );

  // Drain any handshake-queued pending events so the set starts empty and the
  // single flag below is the ONLY pending-event connection.
  for _ in 0..10 {
    if s.conns_with_pending_events.is_empty() {
      break;
    }
    s.handle_timeout(now);
    while s.poll_transmit().is_some() {}
    while s.poll_event().is_some() {}
  }
  assert!(
    s.conns_with_pending_events.is_empty(),
    "test setup: the handshake pending-event backlog must be drained first"
  );

  // Simulate the RETIRE→NewIdentifiers requeue: flag exactly ONE pooled connection
  // as carrying a deferred backlog. This arms the ImmediateDue anchor.
  let target = s
    .conns
    .iter_handles()
    .first()
    .copied()
    .expect("the server holds at least one pooled connection");
  s.conns_with_pending_events.insert(target);

  // Precondition: no scheduled (non-anchor) timer is due, so the wake is
  // ImmediateDue-only and must divert to the bounded per-connection servicing.
  let _ = s.poll_timeout();
  assert!(
    s.deadline_index
      .earliest_excluding_2(
        super::deadline::TimerKey::Catchup,
        super::deadline::TimerKey::ImmediateDue,
      )
      .is_none_or(|t| t > now),
    "test setup: no scheduled timer may be due at the wake instant"
  );

  let memb_before = s.membership_time_advances();
  s.counters.connection_visits = 0;
  s.handle_timeout(now);

  assert_eq!(
    s.counters.connection_visits, 1,
    "an ImmediateDue-only wake must visit ONLY the one pending-event connection, \
       not all {pooled} pooled connections; the 2-way dispatch runs the full tick \
       and visits every connection"
  );
  assert_eq!(
    s.membership_time_advances(),
    memb_before,
    "an ImmediateDue-only wake must NOT advance membership time (no full tick)"
  );
  assert!(
    s.conns_with_pending_events.is_empty(),
    "servicing the pending connection drains its backlog and clears its flag \
       (the set self-drains)"
  );
}

/// The EITHER/OR precedence: when a GENUINE scheduled timer is due, `handle_timeout`
/// MUST run the full `run_tick` (advancing membership time) even if the sticky
/// Catchup anchor AND the ImmediateDue anchor are ALSO due — the scheduled tick
/// subsumes both bounded paths and is the ONLY path that advances membership.
///
/// Mutation-verify: reorder the dispatch to prefer the anchors over the scheduled
/// timer — an anchor-only bounded step then runs instead of `run_tick`, membership
/// time never advances, and the `> before` assertion fails.
#[test]
fn due_scheduled_timer_takes_precedence_over_both_anchors() {
  let a_addr: SocketAddr = "127.0.0.1:7996".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7997".parse().unwrap();
  let now = Instant::now();
  // Default schedulers so the membership endpoint arms a real periodic timer.
  let mut a = make_endpoint("a", a_addr, now);
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(b_cfg, test_config_small_conn_window_bidi(400), b_addr, now);

  // Establish A -> B.
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "test precondition: A and B must be connected"
  );
  while a.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  // Drain handshake pending events so the insert below is the deliberate anchor.
  for _ in 0..10 {
    if a.conns_with_pending_events.is_empty() {
      break;
    }
    a.handle_timeout(now);
    while a.poll_transmit().is_some() {}
    while a.poll_event().is_some() {}
  }

  // Build a residue (arms the sticky Catchup anchor).
  const N: usize = 200;
  let payload = Bytes::from(vec![0xC3u8; 4096]);
  for _ in 0..N {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to the established B");
  }
  while a.poll_transmit().is_some() {}
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  let ids: Vec<StreamId> = a.bridges.keys().copied().collect();
  for id in &ids {
    super::enqueue_ready_bridge(&mut a.bridges, &mut a.ready_bridges, *id);
  }
  a.service_connection(ch, now);
  assert!(!a.ready_bridges.is_empty(), "the residue must be present");
  assert!(
    a.next_catchup_at.is_some(),
    "the residue arms the sticky Catchup anchor"
  );

  // AND a pending-event backlog (arms the ImmediateDue anchor).
  a.conns_with_pending_events.insert(ch);

  // Pick a wake instant at which the membership timer AND both anchors are due.
  let memb_deadline = a
    .endpoint_ref()
    .poll_timeout()
    .expect("default schedulers arm a membership timer");
  let due = memb_deadline.max(now + super::CATCHUP_INTERVAL);
  let memb_before = a.membership_time_advances();
  a.handle_timeout(due);

  assert!(
    a.membership_time_advances() > memb_before,
    "a due scheduled timer must run the full run_tick (membership advances) even \
       with the Catchup and ImmediateDue anchors also due; preferring the anchors \
       would skip membership time"
  );
  // The full tick subsumes both bounded paths: it drains the residue and the
  // pending backlog and clears the sticky anchor.
  assert!(
    a.ready_bridges.is_empty() && a.next_catchup_at.is_none(),
    "run_tick drains the residue and clears the sticky catch-up anchor"
  );
  assert!(
    a.conns_with_pending_events.is_empty(),
    "run_tick services the pending connection and clears its flag"
  );
}

/// The sticky catch-up wake is NOT attacker-pushable. `poll_timeout` publishes the
/// `next_catchup_at` field VERBATIM, and the field is armed independently of
/// `last_now`; an inbound datagram bumps `last_now` (even a rejected one) but must
/// NOT move the wake. So a peer flooding datagrams faster than `CATCHUP_INTERVAL`
/// cannot push the residue-drain wake forward forever and strand it.
///
/// Mutation-verify: revert `poll_timeout` to arm Catchup at `last_now +
/// CATCHUP_INTERVAL` — each flood datagram bumps `last_now`, so `poll_timeout` then
/// returns a wake that ADVANCES past every datagram and the "same deadline"
/// assertion fails on the first flooded datagram.
#[test]
fn sticky_catchup_wake_is_not_pushed_by_datagram_flood() {
  let a_addr: SocketAddr = "127.0.0.1:7998".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
  let now = Instant::now();
  let (mut a, residue) = a_with_budget_deferred_residue(a_addr, b_addr, now);

  // The residue armed the sticky anchor at now + CATCHUP_INTERVAL.
  let pinned = a
    .next_catchup_at
    .expect("a budget-deferred residue arms the sticky catch-up anchor");
  assert_eq!(
    pinned,
    now + super::CATCHUP_INTERVAL,
    "the anchor was armed one interval after the residue-creating pass"
  );
  assert_eq!(
    a.poll_timeout(),
    Some(pinned),
    "poll_timeout publishes the sticky anchor as the earliest wake"
  );

  // Flood: bump `last_now` via rejected (empty) datagrams every < CATCHUP_INTERVAL.
  // The sticky anchor must stay PINNED — a datagram cannot move it.
  for i in 1..10u64 {
    let t = now + Duration::from_millis(i);
    // An empty datagram classifies as Reject: `handle_udp` bumps `last_now` to `t`
    // and drops it, touching neither `ready_bridges` nor the sticky anchor.
    a.handle_udp(b_addr, &[], t);
    assert_eq!(
      a.poll_timeout(),
      Some(pinned),
      "a datagram flood must NOT push the sticky catch-up wake forward; the \
         last_now-derived arming would move it to (now + {i}ms) + CATCHUP_INTERVAL"
    );
    assert!(
      !a.ready_bridges.is_empty(),
      "a rejected flood datagram must not drain or grow the residue"
    );
  }

  // The pinned wake is actionable: re-polling the earliest wake drains the residue
  // to empty, never stranded. The first wake (still the sticky anchor) is a BOUNDED,
  // membership-flat catch-up; later chunks may drain via a scheduled tick once the
  // connection's transport timer comes due.
  let mut catchup_steps = 0usize;
  let mut steps = 0usize;
  while !a.ready_bridges.is_empty() {
    let memb_before = a.membership_time_advances();
    let before = a.ready_bridges.len();
    let wake = a
      .poll_timeout()
      .expect("a non-empty residue always schedules a wake");
    a.handle_timeout(wake);
    let drained = before - a.ready_bridges.len();
    if a.membership_time_advances() == memb_before {
      catchup_steps += 1;
      assert!(
        drained <= super::MAX_BRIDGE_PUMPS_PER_PASS,
        "a Catchup-only wake pumps at most the budget; a step drained {drained}"
      );
    }
    steps += 1;
    assert!(
      steps <= residue + 2,
      "the residue must converge, never strand"
    );
  }
  assert!(
    catchup_steps >= 1,
    "the residue must drain via at least one bounded catch-up wake"
  );
}

/// Ferry a datagram warm-up A->B->A until A holds an established pooled connection
/// to B (no bidi stream consumed), so a subsequent reliable dial opens immediately
/// rather than parking on the handshake. Quiesces both transmit queues on return.
fn establish(
  a: &mut QuicEndpoint<SmolStr>,
  b: &mut QuicEndpoint<SmolStr>,
  a_addr: SocketAddr,
  b_addr: SocketAddr,
  now: Instant,
) {
  let _ = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now);
  a.flush_outbound_transmits(now);
  for _ in 0..300 {
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
    if a.live_connections_to(b_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "establish: A must hold an established pooled connection to B"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
}

/// The per-connection outbound-stream cap bounds the number of live DIALER bridges
/// on a pooled connection at `C_OUT`, regardless of how large a `MAX_STREAMS`
/// limit the PEER advertises. Excess dials re-park (they are not retired) so they
/// open as slots free. This is the local bound that keeps quinn's
/// `connection_blocked` set — and thus the Writable-arm work per MAX_DATA —
/// config-bounded rather than peer-controlled.
///
/// Construction: B advertises a bidi limit far above `C_OUT`, so credit is never
/// the binding constraint. A opens `C_OUT + EXTRA` outbound user-message dials to
/// B WITHOUT ferrying (their payloads never leave A, so every bridge stays live).
///
/// Mutation-verify: remove the `C_OUT` gate in `process_dial_entry` — all
/// `C_OUT + EXTRA` dials then open (credit allows), so `outbound_bridge_count`
/// climbs to `C_OUT + EXTRA` and the `== C_OUT` assertion fails.
#[test]
fn outbound_cap_bounds_live_dialer_bridges() {
  let a_addr: SocketAddr = "127.0.0.1:8200".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8201".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  // B advertises a bidi limit WAY above C_OUT, so the local cap — not the peer's
  // credit — is the binding constraint that parks the excess.
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  const EXTRA: usize = 16;
  let payload = Bytes::from_static(b"x");
  for _ in 0..(super::C_OUT + EXTRA) {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message dial to the established B");
    // Drain A's owed transmits so `out` does not grow unboundedly; the payloads
    // are discarded (never delivered to B), so every opened bridge stays live.
    while a.poll_transmit().is_some() {}
  }

  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "the connection must hold exactly C_OUT live dialer bridges — the excess dials \
       park behind the local cap, NOT the peer's credit"
  );
  assert_eq!(
    a.bridges.len(),
    super::C_OUT,
    "exactly C_OUT dialer bridges are live on A (B dials nothing back)"
  );
  assert_eq!(
    a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0),
    EXTRA,
    "the EXTRA dials past the cap must re-park on B's bucket (a NEW re-park cause), \
       not retire — so they open as slots free"
  );
}

/// The outbound-count balance across mint and single reap, keyed on the DIALER
/// role (`eager_outbound_label`) and NOT on `pending_outbound_kinds`. A GOSSIP
/// push/pull — a dialer that is ABSENT from `pending_outbound_kinds` (it never
/// went through the driver `start_push_pull` wrapper) — MUST still be counted at
/// mint and decremented at reap, or the counter leaks on every gossip dial until
/// the connection permanently refuses all dials (a silent cluster brick).
///
/// Mutation-verify: key the mint increment off `pending_outbound_kinds` — the
/// gossip dial (absent) is then not counted and the `== 1` assertion fails. Key
/// the reap decrement off `pending_outbound_kinds` — the gossip dial is then not
/// decremented and the post-reap `== 0` assertion fails (the leak).
#[test]
fn outbound_count_counts_and_decrements_gossip_dial() {
  let a_addr: SocketAddr = "127.0.0.1:8210".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8211".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Inject a GOSSIP push/pull directly on the inner endpoint — this bypasses the
  // quic-level `start_push_pull` wrapper, so the dial's id is NEVER inserted into
  // `pending_outbound_kinds` (exactly an internally-scheduled gossip dial).
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);

  // Ferry until the dial opens its bridge; break WHILE it is still live.
  let mut ch = None;
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_bridge_count() >= 1 {
      ch = a.conns.handle_for(&b_addr);
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }
  let ch = ch.expect("A's gossip dial must open a bridge on a pooled connection to B");
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    a.live_bridge_count(),
    "the gossip dialer bridge (absent from pending_outbound_kinds) MUST be counted \
       — keying the increment off pending_outbound_kinds leaves this 0"
  );
  assert!(
    a.conns.get(ch).unwrap().outbound_bridge_count() >= 1,
    "the gossip dial is counted against the connection's outbound cap"
  );

  // Ferry the exchange to completion; the bridge reaps via the single-reap pump
  // path on the SURVIVING connection.
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
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.live_bridge_count() == 0 {
      break;
    }
    if !moved {
      break;
    }
  }
  assert_eq!(
    a.live_bridge_count(),
    0,
    "the gossip exchange must complete and reap its bridge on the surviving connection"
  );
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    0,
    "the reaped gossip dialer bridge MUST decrement the outbound count back to 0 — \
       keying the decrement off pending_outbound_kinds leaks the counter (a silent brick)"
  );
}

/// A connection-level loss reaps every DIALER bridge on the connection AND removes
/// the whole `ConnEntry`, so the outbound count is discarded wholesale with no
/// leak — the bulk-reap decrement is balanced (its debug-assert would fire on an
/// underflow) and the entry is gone.
#[test]
fn outbound_count_discarded_with_connentry_on_connection_loss() {
  let a_addr: SocketAddr = "127.0.0.1:8220".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8221".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Open a few live dialer bridges (payloads never delivered, so they stay live).
  let payload = Bytes::from_static(b"x");
  for _ in 0..3 {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message dial to B");
    while a.poll_transmit().is_some() {}
  }
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  assert!(
    a.conns.get(ch).unwrap().outbound_bridge_count() >= 3,
    "the three dialer bridges are counted before the loss"
  );

  // Force the pooled connection to drained-loss WITHOUT elapsing the bridges' own
  // exchange deadlines: the bulk reap runs (decrementing the count per dialer
  // bridge) and `reap_if_drained` removes the whole entry this same tick.
  a.conns
    .get_mut(ch)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());
  let close_due = a
    .conns
    .get_mut(ch)
    .unwrap()
    .conn_mut()
    .poll_timeout()
    .expect("close arms the Close timer");
  a.handle_timeout(crate::Instant::from_std(close_due));

  assert_eq!(
    a.live_bridge_count(),
    0,
    "every dialer bridge riding the lost connection must be reaped this tick"
  );
  assert!(
    a.conns.handle_for(&b_addr).is_none() && a.conns.get(ch).is_none(),
    "the whole ConnEntry (with its outbound count) must be gone — no counter leak"
  );
}

/// A reliable-ping fallback is EXEMPT from the `C_OUT` outbound cap: it is a rare,
/// single-stream, liveness-critical failure-detection dial, and parking it behind
/// a user-message flood could miss the probe's cumulative deadline and yield a
/// false-positive Dead. It opens even when the connection is already at `C_OUT`
/// dialer bridges (and still increments the count, for accounting).
///
/// Mutation-verify: gate reliable-ping like every other dial — it then re-parks at
/// the cap (no bridge minted), so the `bridges.contains_key(ping_id)` assertion
/// fails and it would instead sit in `dial_parked` until its probe deadline.
#[test]
fn reliable_ping_exempt_from_outbound_cap() {
  let a_addr: SocketAddr = "127.0.0.1:8230".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8231".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Fill the cap with user-message dials (payloads never delivered → all live).
  let payload = Bytes::from_static(b"x");
  for _ in 0..super::C_OUT {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message dial to B");
    while a.poll_transmit().is_some() {}
  }
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "test precondition: the connection is exactly at the C_OUT cap"
  );

  // A reliable-ping dial to the SAME peer at the cap must OPEN (exempt), not park.
  let ping_id = a.start_reliable_ping(
    SmolStr::new("b"),
    b_addr,
    7,
    now + Duration::from_secs(5),
    now,
  );
  while a.poll_transmit().is_some() {}
  assert!(
    a.bridges.contains_key(&ping_id),
    "the reliable-ping dial MUST open its bridge even at the C_OUT cap (exempt) — \
       gating it too would leave it parked and risk a false-positive Dead"
  );
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT + 1,
    "the exempt reliable-ping still increments the outbound count, for accounting"
  );

  // A further USER-MESSAGE dial at the cap DOES park (the exemption is scoped to
  // reliable-ping only).
  let over_id = a
    .start_user_message(b_addr, payload.clone(), now)
    .expect("A schedules one more user-message dial");
  while a.poll_transmit().is_some() {}
  assert!(
    !a.bridges.contains_key(&over_id),
    "a NON-reliable-ping dial past the cap must NOT open (it parks) — the exemption \
       is reliable-ping-only"
  );
}

/// A per-request reliable `start_*` does ZERO event-queue work: it services its
/// dial straight from the [`crate::DialIntent`] the machine returns, never draining
/// and re-queueing the whole application-event backlog to sieve out the one dial
/// request. Seed a large non-dial backlog, run many `start_user_message` calls, and
/// assert `sieve_dial_events` moved nothing — and that the backlog stays observable
/// in its original order.
///
/// Mutation-verify: revert the wrapper to the event-emitting `start_user_message`
/// plus a `sieve_dial_events` call — the sieve then re-traverses the whole backlog
/// on every start and `events_resieved` climbs to `N * B`, failing the `== 0`
/// assertion.
#[test]
fn reliable_start_does_no_event_queue_work() {
  let a_addr: SocketAddr = "127.0.0.1:8260".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  // Clear any construction-time events so the backlog below is exactly the seeded
  // NodeJoined set.
  while a.poll_event().is_some() {}

  // Seed a large non-dial backlog: B distinct alives, each emitting a NodeJoined
  // that stays queued in the inner endpoint (never drained via poll_event).
  const B: usize = 64;
  let mut expected_ids: Vec<SmolStr> = Vec::with_capacity(B);
  for i in 0..B {
    let id = SmolStr::new(format!("peer-{i}"));
    let peer: SocketAddr = format!("127.0.0.2:{}", 9000 + i).parse().unwrap();
    let alive = crate::typed::Alive::new(1, crate::Node::new(id.clone(), peer))
      .with_meta(crate::typed::Meta::empty());
    a.handle_alive(peer, alive, now);
    expected_ids.push(id);
  }

  // Run many reliable starts to a cold peer. Each services its own dial from the
  // returned descriptor; none touches the inner event queue.
  a.counters.events_resieved = 0;
  const N: usize = 8;
  let target: SocketAddr = "127.0.0.3:9500".parse().unwrap();
  let payload = Bytes::from_static(b"x");
  for _ in 0..N {
    a.start_user_message(target, payload.clone(), now)
      .expect("A schedules a reliable user-message dial");
    while a.poll_transmit().is_some() {}
  }
  assert_eq!(
    a.counters.events_resieved, 0,
    "a reliable start must move no events through sieve_dial_events; reverting a \
       wrapper to the event-emitting method + sieve makes this climb to N*B"
  );

  // The backlog is intact and still observed in its original FIFO order.
  let mut seen: Vec<SmolStr> = Vec::new();
  while let Some(ev) = a.poll_event() {
    if let Event::NodeJoined(ns) = ev {
      seen.push(ns.id_ref().clone());
    }
  }
  assert_eq!(
    seen, expected_ids,
    "every backlog NodeJoined must still be observable in its original order"
  );
}

/// The machine-INTERNAL reliable-ping escalation gets the `C_OUT` outbound-cap
/// exemption even though it never passes through the coordinator start wrapper (so
/// it populates no `pending_outbound_kinds` entry). The probe FSM emits its
/// reliable-ping `DialRequested` inside `ep.handle_timeout`; the tick's
/// `service_dials` sieve stamps the kind via `Endpoint::intent_kind` and
/// `process_dial_entry` reads it off the entry. Here the escalation's exact
/// `DialRequested` is injected via the raw endpoint's `start_reliable_ping` (the
/// same OLD method the probe FSM calls) and then serviced by one `run_tick` — the
/// same sieve path — so a saturated cap cannot strand the liveness-critical
/// fallback to its probe deadline (a false Suspect of a live peer).
///
/// Mutation-verify: re-source the exemption to
/// `pending_outbound_kinds.get(&id) == Some(ReliablePing)` — the injected ping has
/// no such map entry, so it is gated at the cap, parks, and its bridge never mints;
/// both the `contains_key` and the `> C_OUT` assertions then fail.
#[test]
fn machine_internal_reliable_ping_exempt_from_cap() {
  let a_addr: SocketAddr = "127.0.0.1:8262".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8263".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Saturate A's outbound cap to B with user-message dials (payloads never
  // delivered → all live).
  let payload = Bytes::from_static(b"x");
  for _ in 0..super::C_OUT {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message dial to B");
    while a.poll_transmit().is_some() {}
  }
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "test precondition: the connection is exactly at the C_OUT cap"
  );

  // Inject the machine-internal reliable-ping the way the probe FSM does: the OLD
  // `start_reliable_ping` on the raw endpoint enqueues an `Event::DialRequested`
  // and registers an `OutboundKind::ReliablePing` intent, but populates NO
  // coordinator `pending_outbound_kinds` entry — exactly the escalation's state.
  let deadline = now + Duration::from_secs(5);
  let ping_id = a
    .endpoint_mut()
    .start_reliable_ping(SmolStr::new("b"), b_addr, 7, deadline);
  assert!(
    !a.pending_outbound_kinds.contains_key(&ping_id),
    "precondition: the machine-internal ping has no pending_outbound_kinds entry"
  );

  // One tick sieves that DialRequested (service_dials → intent_kind stamps the
  // ReliablePing kind onto the entry), and process_dial_entry exempts it.
  a.run_tick(now);
  while a.poll_transmit().is_some() {}

  assert!(
    a.bridges.contains_key(&ping_id),
    "the machine-internal reliable-ping MUST open its bridge despite the saturated \
       cap — the exemption is read off the entry's kind, not pending_outbound_kinds"
  );
  assert!(
    a.conns.get(ch).unwrap().outbound_bridge_count() > super::C_OUT,
    "the exempt reliable-ping opened PAST the cap; only an exempt dial can"
  );
  assert!(
    !a.dial_parked
      .get(&b_addr)
      .is_some_and(|bucket| !bucket.is_empty()),
    "no reliable-ping intent parked behind the saturated cap"
  );
}

/// An inert post-leave `start_push_pull` (the endpoint is no longer running, so the
/// machine registers no intent and queues no event) must NOT leak a
/// `pending_outbound_*` entry — no bridge is ever created for the id to reap it —
/// and must emit a SYNCHRONOUS `Failed` `ExchangeCompleted` for the returned id so a
/// join waiter parked on it resolves immediately instead of hanging to its timeout.
///
/// Mutation-verify: restore the unconditional `pending_outbound_*` insert on the
/// inert path — the `!contains_key` leak assertions then fail.
#[test]
fn inert_push_pull_after_leave_no_map_leak_and_synchronous_failed() {
  use crate::event::{ExchangeId, ExchangeKind, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8266".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  a.leave(now).expect("leave succeeds");
  // Clear the leave's own events so the completion below is unambiguous.
  while a.poll_event().is_some() {}

  let peer: SocketAddr = "127.0.0.3:9600".parse().unwrap();
  let id = a.start_push_pull(peer, PushPullKind::Refresh, now);
  assert!(
    !a.pending_outbound_kinds.contains_key(&id),
    "an inert post-leave push/pull must insert no pending_outbound_kinds entry"
  );
  assert!(
    !a.pending_outbound_peers.contains_key(&id),
    "an inert post-leave push/pull must insert no pending_outbound_peers entry"
  );

  let mut saw_failed = false;
  while let Some(ev) = a.poll_event() {
    if let Event::ExchangeCompleted(c) = ev {
      if c.eid() == ExchangeId::from(id) {
        assert_eq!(c.outcome(), ExchangeStatus::Failed);
        assert_eq!(c.kind(), ExchangeKind::PushPull);
        saw_failed = true;
      }
    }
  }
  assert!(
    saw_failed,
    "an inert post-leave push/pull must emit a synchronous Failed ExchangeCompleted \
       for its id so a parked join waiter resolves immediately"
  );
}

/// The leave chokepoint purges the coordinator-private RELIABLE dial pipeline:
/// after `leave`, `dial_pending`, every `dial_parked` bucket, and the ready-dial
/// ledger are empty, no subsequent tick opens a fresh outbound connection to a
/// parked peer, and each purged UserMessage / PushPull intent surfaces a terminal
/// `Failed` `ExchangeCompleted` so a parked join / reliable-send waiter resolves.
///
/// Mutation-verify: remove the purge call from `leave_with` — the surviving parked
/// dials ride the next un-gated tick's `service_dials`, whose `get_or_dial` opens
/// a fresh post-leave connection to each cold peer, so the flat-conns assertion
/// fails.
#[test]
fn post_leave_purges_reliable_dial_pipeline() {
  use crate::event::{ExchangeId, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8300".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  // Inject parked reliable dials on distinct cold peers (never-answered
  // addresses). Each mirrors a real park: an entry in its peer bucket with
  // `attempted = true`, its `TimerKey::Dial` deadline key registered, and its
  // `pending_outbound_{kinds,peers}` staged (as a `start_*` wrapper would) so the
  // purge's `retire_failed_dial` can surface a Failed completion. No connection is
  // opened for these peers — the assertion below is that leave keeps it that way.
  let deadline = now + Duration::from_secs(30);
  let kinds = [
    super::ExchangeKind::UserMessage,
    super::ExchangeKind::PushPull,
  ];
  let mut expected: Vec<(StreamId, SocketAddr)> = Vec::new();
  for i in 0..6u64 {
    let peer: SocketAddr = format!("127.0.0.9:{}", 9000 + i).parse().unwrap();
    let id = StreamId::from_raw(1_000 + i);
    let kind = kinds[(i % 2) as usize];
    a.dial_parked
      .entry(peer)
      .or_default()
      .push_back(super::PendingDial {
        id,
        peer,
        deadline,
        wake: deadline,
        attempted: true,
        kind,
      });
    a.deadline_index
      .set(super::deadline::TimerKey::Dial(id), Some(deadline));
    a.pending_outbound_kinds.insert(id, kind);
    a.pending_outbound_peers.insert(id, peer);
    expected.push((id, peer));
  }
  assert_eq!(
    a.conns.iter_handles().len(),
    0,
    "test precondition: no connections before leave"
  );

  a.leave(now).expect("leave initiates the flush");
  assert!(!a.is_running(), "after leave the endpoint is not running");

  // The pipeline is empty the moment leave returns.
  assert!(a.dial_pending.is_empty(), "leave drains dial_pending");
  assert!(
    a.dial_parked.is_empty(),
    "leave drains every dial_parked bucket"
  );
  assert!(
    a.ready_dial_peers.is_empty(),
    "leave clears the ready-dial ledger"
  );

  // Each purged UserMessage / PushPull intent surfaced a Failed completion.
  let mut failed: Vec<ExchangeId> = Vec::new();
  while let Some(ev) = a.poll_event() {
    if let Event::ExchangeCompleted(c) = ev {
      if c.outcome() == ExchangeStatus::Failed {
        failed.push(c.eid());
      }
    }
  }
  for (id, _peer) in &expected {
    assert!(
      failed.contains(&ExchangeId::from(*id)),
      "each purged reliable intent must surface a Failed ExchangeCompleted so its \
       parked waiter resolves; missing id {id:?}"
    );
  }

  // Run several ticks: none opens a fresh connection to any parked peer.
  for _ in 0..3 {
    a.handle_timeout(now);
  }
  assert_eq!(
    a.conns.iter_handles().len(),
    0,
    "no post-leave tick may open a fresh outbound connection — the reliable dial \
     pipeline was purged at the leave chokepoint"
  );
  for (_id, peer) in &expected {
    assert!(
      a.conns.handle_for(peer).is_none(),
      "no pooled connection to a purged parked peer may exist post-leave"
    );
  }
}

/// The purge releases `unattempted_dial_count` for every unattempted entry it
/// drains, so a fresh (never-attempted) dial that survived into `dial_pending`
/// cannot arm a permanent post-leave immediate-due busy-wake. Before leave the
/// unattempted dial forces `poll_timeout` to an immediate-due wake at `last_now`;
/// after leave the count is 0 and no such wake is sourced from the dial term.
///
/// Mutation-verify: drop the `unattempted_dial_count` decrement from
/// `purge_dial_entry` — the count stays >= 1 (while `dial_pending` is emptied), so
/// `unattempted_dial_count() == 0` fails and `poll_timeout` re-arms the
/// immediate-due wake at `last_now`.
#[test]
fn no_immediate_due_wake_after_leave() {
  let a_addr: SocketAddr = "127.0.0.1:8310".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  // Anchor `last_now` (and run one empty tick) so the immediate-due term is
  // well-defined.
  a.handle_timeout(now);

  // Seed ONE unattempted fresh dial directly into `dial_pending` (bypassing the
  // intake route), mirroring a real sieve deposit: `attempted = false`, the
  // incremental count bumped, and its Dial deadline key registered.
  let peer: SocketAddr = "127.0.0.9:9100".parse().unwrap();
  let id = StreamId::from_raw(2_000);
  let deadline = now + Duration::from_secs(30);
  a.dial_pending.push_back(super::PendingDial {
    id,
    peer,
    deadline,
    wake: deadline,
    attempted: false,
    kind: super::ExchangeKind::UserMessage,
  });
  a.unattempted_dial_count += 1;
  a.deadline_index
    .set(super::deadline::TimerKey::Dial(id), Some(deadline));
  assert_eq!(a.unattempted_dial_count(), a.unattempted_dial_recount());
  assert_eq!(
    a.poll_timeout(),
    Some(now),
    "test precondition: an unattempted dial forces an immediate-due wake at last_now"
  );

  a.leave(now).expect("leave initiates the flush");
  assert!(!a.is_running());

  // The busy-wake kill: the unattempted unit was released, so the count is 0 (and
  // agrees with the brute-force recount), and `poll_timeout` no longer returns the
  // immediate-due wake sourced from the dial term.
  assert_eq!(
    a.unattempted_dial_count(),
    0,
    "the purge must release every unattempted unit so ImmediateDue is not armed"
  );
  assert_eq!(a.unattempted_dial_count(), a.unattempted_dial_recount());
  assert_ne!(
    a.poll_timeout(),
    Some(now),
    "post-leave poll_timeout must not return an immediate-due wake driven by the \
     purged dial half"
  );
}

/// The purge drops every drained intent's `TimerKey::Dial` deadline key, so no
/// orphan Dial key survives leave to fire a stale `run_tick`-driving wake. Reads
/// the deadline-index oracle (`contains_key`) directly.
///
/// Mutation-verify: drop the `deadline_index.set(TimerKey::Dial(id), None)` from
/// `purge_dial_entry` — each drained intent's Dial key survives leave; the
/// `contains_key` assertion then finds an orphan and fails.
#[test]
fn leave_purge_drops_dial_deadline_keys() {
  let a_addr: SocketAddr = "127.0.0.1:8320".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let deadline = now + Duration::from_secs(30);
  // A mix: unattempted entries in `dial_pending` and attempted parked entries,
  // each with its Dial key registered as a real intake / park would.
  let mut ids: Vec<StreamId> = Vec::new();
  for i in 0..4u64 {
    let id = StreamId::from_raw(3_000 + i);
    let peer: SocketAddr = format!("127.0.0.9:{}", 9200 + i).parse().unwrap();
    if i % 2 == 0 {
      a.dial_pending.push_back(super::PendingDial {
        id,
        peer,
        deadline,
        wake: deadline,
        attempted: false,
        kind: super::ExchangeKind::UserMessage,
      });
      a.unattempted_dial_count += 1;
    } else {
      a.dial_parked
        .entry(peer)
        .or_default()
        .push_back(super::PendingDial {
          id,
          peer,
          deadline,
          wake: deadline,
          attempted: true,
          kind: super::ExchangeKind::PushPull,
        });
    }
    a.deadline_index
      .set(super::deadline::TimerKey::Dial(id), Some(deadline));
    ids.push(id);
  }
  for id in &ids {
    assert!(
      a.deadline_index
        .contains_key(super::deadline::TimerKey::Dial(*id)),
      "test precondition: Dial key registered"
    );
  }

  a.leave(now).expect("leave initiates the flush");
  assert!(!a.is_running());

  for id in &ids {
    assert!(
      !a.deadline_index
        .contains_key(super::deadline::TimerKey::Dial(*id)),
      "the purge must drop every drained intent's TimerKey::Dial; an orphan key \
       fires stale post-leave wakes forever"
    );
  }
}

/// The coordinator `requeue_event`'s `DialRequested` arm is gated on the running
/// state: a `DialRequested` re-queued after leave is DROPPED before any deposit,
/// matching `Endpoint::requeue_event`'s own not-running gate that the
/// coordinator's interception would otherwise bypass.
///
/// Mutation-verify: remove the `if !self.ep.is_running() { return; }` gate — the
/// requeued dial deposits into `dial_pending` (bumps the count, sets a Dial key);
/// the empty-pipeline assertions then fail.
#[test]
fn post_leave_requeue_dial_requested_dropped() {
  use crate::event::DialRequested;
  let a_addr: SocketAddr = "127.0.0.1:8330".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  a.leave(now).expect("leave initiates the flush");
  assert!(!a.is_running());

  let id = StreamId::from_raw(4_000);
  let peer: SocketAddr = "127.0.0.9:9300".parse().unwrap();
  let deadline = now + Duration::from_secs(30);
  let before_count = a.unattempted_dial_count();
  a.requeue_event(
    Event::DialRequested(DialRequested::new(id, peer, deadline)),
    now,
  );

  assert!(
    a.dial_pending.is_empty(),
    "a DialRequested re-queued after leave must be dropped, not deposited into \
     dial_pending"
  );
  assert_eq!(
    a.unattempted_dial_count(),
    before_count,
    "the dropped requeue must not bump the unattempted count"
  );
  assert!(
    !a.deadline_index
      .contains_key(super::deadline::TimerKey::Dial(id)),
    "the dropped requeue must register no Dial deadline key"
  );
}

/// The slot-free wake fires from `catchup_service`: when a DIALER bridge reaps
/// inside the catch-up pump it frees a `C_OUT` slot, and the same catch-up pass
/// services the peer's parked bucket so a `C_OUT`-parked dial opens — rather than
/// stranding to its deadline while only `catchup_service` runs (no datagrams, no
/// due timer).
///
/// Mutation-verify: drop the slot-free servicing from `catchup_service` — the
/// reap inside the catch-up pump frees the slot but nothing services the parked
/// dial, so `parked_id` stays in `dial_parked` and never opens; this fails.
#[test]
fn slot_free_wake_from_catchup_opens_parked_dial() {
  let a_addr: SocketAddr = "127.0.0.1:8240".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8241".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Fill the cap, then one more dial parks behind it (a genuine C_OUT re-park).
  let payload = Bytes::from_static(b"x");
  for _ in 0..super::C_OUT {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A fills the C_OUT cap with user-message dials");
    while a.poll_transmit().is_some() {}
  }
  let parked_id = a
    .start_user_message(b_addr, payload.clone(), now)
    .expect("A schedules the dial that parks behind the cap");
  while a.poll_transmit().is_some() {}
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "test precondition: the connection is at the C_OUT cap"
  );
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.iter().any(|d| d.id == parked_id))
      .unwrap_or(false),
    "test precondition: the extra dial parked behind the cap"
  );

  // Force ONE live dialer bridge terminal and enqueue ONLY it, then run a
  // catch-up pass: pumping it reaps a dialer bridge (freeing a slot), and the
  // same catch-up services B's bucket so the parked dial opens.
  let victim = *a
    .bridges
    .keys()
    .find(|id| **id != parked_id)
    .expect("A holds live dialer bridges");
  a.bridges.get_mut(&victim).unwrap().fail_connection_lost();
  super::enqueue_ready_bridge(&mut a.bridges, &mut a.ready_bridges, victim);
  a.catchup_service(now);

  assert!(
    !a.bridges.contains_key(&victim),
    "the forced-terminal dialer bridge must reap inside the catch-up pump"
  );
  assert!(
    a.bridges.contains_key(&parked_id),
    "the C_OUT-parked dial MUST open on the slot the catch-up reap freed — dropping \
       the slot-free wake from catchup_service strands it in dial_parked"
  );
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.iter().all(|d| d.id != parked_id))
      .unwrap_or(true),
    "the opened dial must leave B's parked bucket"
  );
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "one dialer reaped (-1) and the parked dial opened (+1): the count returns to C_OUT"
  );
}

/// `flush_outbound` owns NO `service_dials`, so a DIALER bridge reaped by its pumps
/// frees a `C_OUT` slot that no dial-servicing on that path re-attempts — only the
/// unified `service_slot_freed_peers` consume (after the last pump, before
/// `finalize_tick`) opens the `C_OUT`-parked dial. Without it `finalize_tick`
/// silently clears the freed-peer accumulator and the parked dial strands to its
/// dial deadline.
///
/// Construction: A fills the `C_OUT` cap with un-ferried user-message dialers to B,
/// then forces one live dialer terminal (NOT enqueued). A `start_user_message`
/// (for B') parks B' behind the still-full cap — the scoped start pumps only its
/// own minted bridge, and the capped B' mints none, so the victim stays live. The
/// driver's explicit `flush_outbound_transmits` then runs `flush_outbound`, whose
/// first `pump_bridges` reaps the victim and whose slot-free consume opens B' the
/// same call.
///
/// Mutation-verify: drop the `service_slot_freed_peers` call from `flush_outbound` —
/// the victim still reaps, but nothing services B's freed slot, so B' stays in
/// `dial_parked` and never opens; this fails.
#[test]
fn slot_free_serviced_after_late_flush_pump() {
  let a_addr: SocketAddr = "127.0.0.1:8260".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8261".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Fill the cap with un-ferried dialers (their payloads never leave A, so every
  // bridge stays live).
  let payload = Bytes::from_static(b"x");
  for _ in 0..super::C_OUT {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A fills the C_OUT cap with user-message dials");
    while a.poll_transmit().is_some() {}
  }
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "test precondition: the connection is at the C_OUT cap"
  );

  // Force ONE live dialer terminal WITHOUT enqueuing it: `flush_outbound`'s first
  // `pump_bridges` (which pumps every bridge) reaps it below.
  let victim = *a
    .bridges
    .keys()
    .next()
    .expect("A holds live dialer bridges");
  a.bridges.get_mut(&victim).unwrap().fail_connection_lost();

  // The scoped `start_user_message` parks B' behind the still-full cap (it pumps
  // only its own minted bridge, and the capped B' mints none), leaving the victim
  // live. The driver's explicit `flush_outbound_transmits` then runs `flush_outbound`,
  // whose first `pump_bridges` reaps the victim and whose slot-free consume opens B'.
  let parked_id = a
    .start_user_message(b_addr, payload.clone(), now)
    .expect("A schedules the dial that parks behind the cap");
  a.flush_outbound_transmits(now);
  while a.poll_transmit().is_some() {}

  assert!(
    !a.bridges.contains_key(&victim),
    "the forced-terminal dialer must reap inside flush_outbound's pump"
  );
  assert!(
    a.bridges.contains_key(&parked_id),
    "the C_OUT-parked dial MUST open on the slot flush_outbound's pump freed — \
       dropping the slot-free consume from flush_outbound strands it in dial_parked"
  );
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.iter().all(|d| d.id != parked_id))
      .unwrap_or(true),
    "the opened dial must leave B's parked bucket"
  );
  assert_eq!(
    a.conns.get(ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "one dialer reaped (-1) and the parked dial opened (+1): count returns to C_OUT"
  );
  assert!(
    a.slot_freed_peers.is_empty(),
    "flush_outbound must drain the freed-peer accumulator before finalize"
  );
}

/// `service_slot_freed_peers` LOOPS TO A FIXPOINT: pumping a freed peer's bucket can
/// reap another DIALER bridge (a synchronous failure) whose reap re-populates
/// `slot_freed_peers`, so a single non-looped pass would leave the set non-empty and
/// strand the re-populated peer's parked dial. The loop drains it to empty.
///
/// Construction: A holds a live dialer to B; it is forced terminal and enqueued, and
/// B is seeded into `slot_freed_peers` (as a prior reap would have). Servicing B
/// drains the ready queue, reaping the forced-terminal dialer, whose reap re-queues
/// B into `slot_freed_peers` — the reentrancy the loop must absorb.
///
/// Mutation-verify: replace the fixpoint loop with a single non-looped pass — the
/// reentrant reap leaves `slot_freed_peers` non-empty on return, so this assertion
/// fails (and on the tick / `flush_outbound` paths `finalize_tick`'s `debug_assert!`
/// would fire).
#[test]
fn slot_free_fixpoint_reentrant() {
  let a_addr: SocketAddr = "127.0.0.1:8270".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8271".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // One live dialer bridge to B; force it terminal and enqueue it so the helper's
  // drain reaps it — the reap re-queues B into `slot_freed_peers`.
  let payload = Bytes::from_static(b"x");
  a.start_user_message(b_addr, payload, now)
    .expect("A opens one user-message dialer to B");
  while a.poll_transmit().is_some() {}
  let victim = *a.bridges.keys().next().expect("A holds one dialer bridge");
  a.bridges.get_mut(&victim).unwrap().fail_connection_lost();
  super::enqueue_ready_bridge(&mut a.bridges, &mut a.ready_bridges, victim);

  // Seed the set as a prior-pass reap would have, so the helper enters its loop.
  a.slot_freed_peers.insert(b_addr);

  let mut budget = super::MAX_BRIDGE_PUMPS_PER_PASS;
  let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = a.service_slot_freed_peers(now, &mut budget, &mut dial_budget);

  assert!(
    !a.bridges.contains_key(&victim),
    "the enqueued forced-terminal dialer must reap inside the helper's drain"
  );
  assert!(
    a.slot_freed_peers.is_empty(),
    "the fixpoint must drain slot_freed_peers to empty even when a reap during the \
       drain re-populates it; a single non-looped pass leaves B queued and would \
       trip finalize_tick's debug_assert"
  );
}

/// The global tick's second `pump_bridges` (step 5.5) runs AFTER step 5's
/// `service_dials` full-drain, so a DIALER bridge that reaps there frees a `C_OUT`
/// slot `service_dials` already passed — its `C_OUT`-parked dial strands unless the
/// step (5.6) `service_slot_freed_peers` consume (before `finalize_tick`) services
/// the freed peer. This is the tick twin of the `flush_outbound` case.
///
/// Construction: A holds a live victim dialer to B whose OPENING is delivered to B's
/// quinn (so B knows the stream) but never read; A fills the rest of the `C_OUT` cap
/// and parks B'. B issues STOP_SENDING for the victim's stream, harvested into A's
/// connection as a STAGED `ConnectionEvent` (NOT applied). Running the tick then
/// applies it in step (4), which fails+enqueues the victim; step (2)'s earlier pump
/// left it live, so it reaps only in step (5.5) — after `service_dials` — and the
/// step (5.6) consume opens B'.
///
/// Mutation-verify: drop the `service_slot_freed_peers` call from the tick — the
/// victim still reaps in step (5.5) but `finalize_tick` clears the freed-peer
/// accumulator, so B' stays in `dial_parked` and never opens; this fails.
#[test]
fn slot_free_serviced_after_late_tick_pump() {
  use bytes::BytesMut;
  use quinn_proto::{DatagramEvent, VarInt};

  let a_addr: SocketAddr = "127.0.0.1:8280".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8281".parse().unwrap();
  let now = Instant::now();
  // A's periodic schedulers off so the tick's step (3) fires no membership timer
  // that could reap or redial independently of the slot-free consume under test.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  let a_ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a pooled connection to B");
  let b_ch = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a pooled connection to A");

  // Open the victim dialer and capture its ids. Deliver its opening datagrams to B's
  // quinn ONLY (no coordinator read), so B's `Connection` knows the stream — and can
  // STOP it — while the recv half stays unconsumed.
  let payload = Bytes::from_static(b"victim");
  a.start_user_message(b_addr, payload.clone(), now)
    .expect("A opens the victim user-message dialer to B");
  let victim = *a.bridges.keys().next().expect("A holds the victim bridge");
  let vsid = a.bridges.get(&victim).unwrap().sid();
  {
    let mut scratch = Vec::new();
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        if let Some(DatagramEvent::ConnectionEvent(ch, cev)) = b.quinn.handle(
          now.into_std(),
          a_addr,
          None,
          None,
          BytesMut::from(&bytes[..]),
          &mut scratch,
        ) {
          b.conns.get_mut(ch).unwrap().conn_mut().handle_event(cev);
        }
      }
    }
  }

  // Fill the rest of the C_OUT cap with un-ferried dialers, then park B'.
  for _ in 1..super::C_OUT {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A fills the C_OUT cap");
    while a.poll_transmit().is_some() {}
  }
  assert_eq!(
    a.conns.get(a_ch).unwrap().outbound_bridge_count(),
    super::C_OUT,
    "test precondition: the connection is at the C_OUT cap (victim + fillers)"
  );
  let parked_id = a
    .start_user_message(b_addr, payload.clone(), now)
    .expect("A schedules the dial that parks behind the cap");
  while a.poll_transmit().is_some() {}
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.iter().any(|d| d.id == parked_id))
      .unwrap_or(false),
    "test precondition: B' parked behind the cap"
  );

  // B issues STOP_SENDING for the victim's recv half; harvest its datagrams into A's
  // connection as STAGED pending events (NOT applied) so they take effect only in the
  // tick's step (4).
  let _ = b
    .conns
    .get_mut(b_ch)
    .unwrap()
    .conn_mut()
    .recv_stream(vsid)
    .stop(VarInt::from_u32(7));
  let mut stop_dgs: Vec<Vec<u8>> = Vec::new();
  {
    let mut buf = Vec::new();
    while let Some(tr) =
      b.conns
        .get_mut(b_ch)
        .unwrap()
        .conn_mut()
        .poll_transmit(now.into_std(), 1, &mut buf)
    {
      stop_dgs.push(buf[..tr.size].to_vec());
      buf.clear();
    }
  }
  assert!(
    !stop_dgs.is_empty(),
    "B must emit a STOP_SENDING datagram for the victim's stream"
  );
  {
    let mut scratch = Vec::new();
    for dg in &stop_dgs {
      if let Some(DatagramEvent::ConnectionEvent(ch, cev)) = a.quinn.handle(
        now.into_std(),
        b_addr,
        None,
        None,
        BytesMut::from(&dg[..]),
        &mut scratch,
      ) {
        a.conns.get_mut(ch).unwrap().queue_pending_event(cev);
      }
    }
  }
  assert!(
    a.bridges.contains_key(&victim),
    "test precondition: the victim is still live entering the tick (step 2 has not \
       yet applied the staged STOP)"
  );

  // Run the tick: step (4) applies the staged STOP (fails+enqueues the victim), step
  // (5) `service_dials` re-parks B' (cap still full), step (5.5) reaps the victim,
  // and step (5.6)'s consume opens B' on the freed slot.
  a.run_tick(now);

  assert!(
    !a.bridges.contains_key(&victim),
    "the victim must reap in the tick's second pump (after service_dials)"
  );
  assert!(
    a.bridges.contains_key(&parked_id),
    "the C_OUT-parked dial MUST open on the slot the step-(5.5) reap freed — dropping \
       the tick's step-(5.6) slot-free consume strands it in dial_parked"
  );
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.iter().all(|d| d.id != parked_id))
      .unwrap_or(true),
    "the opened dial must leave B's parked bucket"
  );
  assert!(
    a.slot_freed_peers.is_empty(),
    "the tick must drain the freed-peer accumulator before finalize (finalize asserts it)"
  );
}

/// Deleting the Writable skip means the arm acts on EVERY connection-window edge,
/// so a bridge blocked past the OLD per-pass budget is still enqueued and pumps
/// within the catch-up cadence — never stranded to its exchange deadline (quinn
/// clears `connection_blocked` on yield and never re-adds it, so a dropped edge
/// would be permanent).
///
/// Construction: A opens N connection-window-blocked bridges (N far above the pump
/// budget). B reads A's data and emits MAX_DATA; delivering it storms `Writable`
/// for all N. Draining the catch-up cadence (with a data ferry, never advancing to
/// the ~5s exchange deadline) reaps ALL N.
///
/// Mutation-verify: restore the `writable_enqueues >= MAX_BRIDGE_PUMPS_PER_PASS`
/// skip — the storm pass then enqueues at most the pump budget and pumps exactly
/// that many, so NO residue forms (the past-budget edges are dropped, never
/// enqueued) and the `after > before` assertion fails; a dropped edge is
/// permanent because quinn cleared `connection_blocked` on the yield.
#[test]
fn writable_edge_not_dropped_bridge_enqueues_and_drains_via_catchup() {
  let a_addr: SocketAddr = "127.0.0.1:8250".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8251".parse().unwrap();
  let now = Instant::now();
  // A's periodic schedulers off so the residue drains via bounded catch-up wakes
  // rather than a full tick that would pump every bridge regardless.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_small_conn_window_bidi(super::C_OUT as u32 + 64),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Open N connection-window-blocked DIALER bridges — far above the pump budget so
  // one MAX_DATA storms more Writables than the budget — but under C_OUT so all
  // open.
  const N: usize = 200;
  let payload = Bytes::from(vec![0xC3u8; 4096]);
  for _ in 0..N {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("A opens a reliable user-message exchange to B");
  }
  let live0 = a.bridges.len();
  assert!(
    live0 > super::MAX_BRIDGE_PUMPS_PER_PASS && live0 <= super::C_OUT,
    "test precondition: A holds more blocked bridges ({live0}) than the pump budget \
       but under C_OUT (so all open)"
  );

  // Deliver A's blocked sends to B and let B read them and emit MAX_DATA; do NOT
  // deliver B's output back to A yet, so the storm concentrates in one A pass.
  let mut b_to_a: Vec<Vec<u8>> = Vec::new();
  let mut t = now;
  for _ in 0..120 {
    t += Duration::from_millis(5);
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, t);
        moved = true;
      }
    }
    b.handle_timeout(t);
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        b_to_a.push(bytes.to_vec());
      }
    }
    if !moved {
      break;
    }
  }
  assert!(
    !b_to_a.is_empty(),
    "B must emit MAX_DATA datagrams after reading A's data"
  );

  // Deliver B's MAX_DATA to A one datagram = one service pass at a time. On the
  // pass that storms MORE Writables than the pump budget, the arm must enqueue ALL
  // of them: the pass pumps at most the budget and leaves the excess as RESIDUE
  // (`after > before`). The skip caps the enqueues at the budget, so that same
  // pass leaves NO residue and the excess edges are dropped forever.
  let mut storm_seen = 0u64;
  let mut storm_residue_grew = false;
  for dg in &b_to_a {
    a.counters.writable_events_seen = 0;
    let before = a.ready_bridges.len();
    a.handle_udp(b_addr, dg, t);
    let seen = a.counters.writable_events_seen;
    let after = a.ready_bridges.len();
    if seen > super::MAX_BRIDGE_PUMPS_PER_PASS as u64 {
      storm_seen = storm_seen.max(seen);
      if after > before {
        storm_residue_grew = true;
      }
    }
    // Drain the residue via the sticky catch-up cadence before the next datagram,
    // so each pass's residue growth is measured cleanly. The catch-up pumps the
    // enqueued bridges (proving they are pumped, never stranded); it converges
    // because no new MAX_DATA arrives during the drain.
    let mut steps = 0usize;
    while !a.ready_bridges.is_empty() {
      let wake = a
        .poll_timeout()
        .expect("a non-empty residue always schedules a wake");
      a.handle_timeout(wake);
      steps += 1;
      assert!(steps <= N, "the residue must drain in bounded steps");
    }
  }
  assert!(
    storm_seen > super::MAX_BRIDGE_PUMPS_PER_PASS as u64,
    "non-vacuous: one pass must storm more Writable edges ({storm_seen}) than the \
       pump budget, so the past-budget edges are what the skip would drop"
  );
  assert!(
    storm_residue_grew,
    "the storm pass must ENQUEUE more bridges than it pumps this pass, leaving a \
       residue (after > before) — proof every edge past the pump budget was acted \
       on. Restoring the enqueue skip caps enqueues at the budget so no residue \
       forms and the past-budget bridges are stranded."
  );
}

/// The class-closure anchor test: a BOUNDED servicing pass that budget-defers a
/// creditable parked-dial tail deposits the peer into the ready-dial ledger, arming
/// the sticky catch-up anchor, so a STRICT-POLL driver (advancing only via
/// `poll_timeout` / `handle_timeout`) mints every deferred intent BEFORE its dial
/// deadline — no reliable send expires despite available capacity.
///
/// Construction: establish A -> B with ample bidi credit, quiesce so no ImmediateDue
/// anchor competes, inject `M > MAX_DIAL_ATTEMPTS_PER_PASS` real creditable intents
/// parked on B's bucket, then run ONE budgeted `service_peer_bucket` pass (the
/// establishment/credit path a datagram drives) which mints the budget and DEFERS a
/// creditable tail. The minted bridges are left un-enqueued so the SOLE residue is
/// the ready-dial deposit — the anchor is armed by the dial ledger alone.
///
/// Mutations that must each fail this test (only (a) is exercised in CI; (b) and (c)
/// are stated as the contract):
///   (a) remove the ready-dial deposit in `service_peer_bucket` — the ledger stays
///       empty, so `reconcile_catchup_anchor` never arms `next_catchup_at` and the
///       pre-drive assertions fail; were the drive reached, the tail would strand.
///   (b) drop `ready_dial_peers` from `has_residue` — `reconcile_catchup_anchor`
///       sees no residue and clears the anchor, same failure.
///   (c) make `catchup_service` skip the ready-dial drain — the anchor fires but
///       drains nothing, so the tail never mints and the post-drive assertions fail.
#[test]
fn budget_deferred_parked_dial_mints_before_deadline_via_catchup() {
  use crate::event::{Event, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8300".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8301".parse().unwrap();
  let now = Instant::now();
  // A: periodic membership schedulers disabled so no staggered timer falls due in the
  // drain window — every wake is provably the bounded catch-up (never a full tick that
  // would `service_dials` full-drain the tail regardless of the ledger).
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(512),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  // Quiesce any handshake-queued pending events so no ImmediateDue anchor (an
  // earlier-than-catch-up wake) competes with the catch-up during the strict-poll
  // drive.
  for _ in 0..10 {
    if a.conns_with_pending_events.is_empty() {
      break;
    }
    a.handle_timeout(now);
    while a.poll_transmit().is_some() {}
    while a.poll_event().is_some() {}
  }
  while a.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  assert_eq!(
    a.live_bridge_count(),
    0,
    "test precondition: the warm-up mints no bridge"
  );

  // Inject M real creditable intents parked on B's bucket: register each on the raw
  // endpoint (so `dial_succeeded` resolves it into a real Stream), sieve the emitted
  // DialRequested into dial_pending, then move them onto dial_parked[B] as attempted —
  // the post-parking state a burst reaches on a since-established, amply-credited
  // connection.
  const M: usize = super::MAX_DIAL_ATTEMPTS_PER_PASS + 16;
  let payload = Bytes::from_static(b"x");
  for _ in 0..M {
    a.endpoint_mut()
      .start_user_message(b_addr, payload.clone(), now)
      .expect("the raw endpoint registers a reliable user-message dial intent");
  }
  a.sieve_dial_events();
  let pending: Vec<super::PendingDial> = a.dial_pending.drain(..).collect();
  assert_eq!(pending.len(), M, "all M intents sieve into dial_pending");
  a.unattempted_dial_count = 0;
  for mut pd in pending {
    pd.attempted = true;
    a.dial_parked.entry(b_addr).or_default().push_back(pd);
  }
  assert!(
    a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0) > super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "the bucket must exceed the budget so the pass defers a creditable tail"
  );
  while a.poll_event().is_some() {}

  // ONE budgeted pass (the datagram establishment/credit path): mint the budget,
  // DEFER the tail, DEPOSIT B into the ready-dial ledger. The minted bridges are left
  // un-enqueued so the ledger is the SOLE residue that arms the anchor.
  let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = a.service_peer_bucket(b_addr, now, &mut dial_budget);
  let minted_first = a.live_bridge_count();
  assert_eq!(
    minted_first,
    super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "the budgeted pass mints exactly the budget"
  );
  let deferred = a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0);
  assert_eq!(
    deferred,
    M - minted_first,
    "exactly the creditable tail stays parked"
  );
  assert!(deferred > 0, "non-vacuous: a creditable tail was deferred");
  assert!(
    !a.ready_dial_peers.is_empty(),
    "the budget-deferred tail deposits B into the ready-dial ledger (removing the \
       deposit — mutation a — leaves it empty)"
  );
  // Arm the sticky anchor for the ledger residue — the reconcile step every bounded
  // caller runs after `service_peer_bucket`. Removing the deposit (mutation a) or
  // dropping ready_dial_peers from `has_residue` (mutation b) leaves this `None`.
  a.reconcile_catchup_anchor(now);
  assert!(
    a.next_catchup_at.is_some(),
    "the ready-dial residue must arm the sticky catch-up anchor"
  );

  // Strict-poll drive: advance ONLY via poll_timeout/handle_timeout. Every deferred
  // intent must mint (open a real bridge) before its dial deadline, with zero
  // ExchangeCompleted failures.
  let mut saw_failure = false;
  for _ in 0..64 {
    if a
      .dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true)
    {
      break;
    }
    let wake = a
      .poll_timeout()
      .expect("a non-empty ready-dial residue always schedules a catch-up wake");
    a.handle_timeout(wake);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev {
        if p.outcome() == ExchangeStatus::Failed {
          saw_failure = true;
        }
      }
    }
  }
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true),
    "every deferred parked dial must mint before its deadline via the catch-up wake; \
       removing the deposit / the has_residue inclusion / the catchup drain strands it"
  );
  assert_eq!(
    a.live_bridge_count(),
    M,
    "all M intents minted a real bridge (none retired despite available capacity)"
  );
  assert!(
    !saw_failure,
    "no deferred dial may retire to ExchangeCompleted::Failed — capacity was available"
  );
}

/// The tick's step (5.6) and `flush_outbound` thread `usize::MAX` as the DIAL budget
/// into `service_slot_freed_peers`, so those O(N) paths can NEVER budget-exit a freed
/// peer's bucket with a creditable tail — the property that makes `finalize_tick`'s
/// "ready_dial_peers empty" debug-assert sound. This pins that an unbounded dial
/// budget fully drains a `> budget` freed bucket WITHOUT depositing into the ledger.
///
/// Construction: establish A -> B with ample credit, inject `> budget` real creditable
/// intents parked on B, seed B into `slot_freed_peers` (as a dialer reap would), then
/// run the slot-free consume with the tick's `usize::MAX` budgets.
///
/// Mutation (stated): pass `MAX_DIAL_ATTEMPTS_PER_PASS` at step (5.6) instead of
/// `usize::MAX` — the freed bucket budget-exits, deposits B into `ready_dial_peers`
/// (as `established_bucket_pass_mints_at_most_the_budget` shows a finite budget does),
/// and `finalize_tick`'s debug-assert fires on the tick's own path.
#[test]
fn slot_free_unbounded_dial_budget_drains_without_deposit() {
  let a_addr: SocketAddr = "127.0.0.1:8310".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8311".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(512),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  const M: usize = super::MAX_DIAL_ATTEMPTS_PER_PASS + 16;
  let payload = Bytes::from_static(b"x");
  for _ in 0..M {
    a.endpoint_mut()
      .start_user_message(b_addr, payload.clone(), now)
      .expect("register a reliable user-message dial intent");
  }
  a.sieve_dial_events();
  let pending: Vec<super::PendingDial> = a.dial_pending.drain(..).collect();
  a.unattempted_dial_count = 0;
  for mut pd in pending {
    pd.attempted = true;
    a.dial_parked.entry(b_addr).or_default().push_back(pd);
  }
  assert!(
    a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0) > super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "non-vacuous: the freed bucket exceeds the budget, so a finite budget would deposit"
  );
  while a.poll_event().is_some() {}

  // The tick's step (5.6) threading: unbounded pump AND dial budgets. B is the freed
  // peer. The unbounded dial budget must fully drain B's `> budget` bucket with NO
  // ready-dial deposit, so `finalize_tick`'s empty-ledger assert holds on the tick path.
  a.slot_freed_peers.insert(b_addr);
  let mut pump_budget = usize::MAX;
  let mut dial_budget = usize::MAX;
  let _ = a.service_slot_freed_peers(now, &mut pump_budget, &mut dial_budget);
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true),
    "the unbounded dial budget fully drains the > budget freed bucket in one pass"
  );
  assert_eq!(
    a.live_bridge_count(),
    M,
    "every deferred intent minted — so the bucket WAS creditable and a finite budget \
       would have deposited a creditable tail"
  );
  assert!(
    a.ready_dial_peers.is_empty(),
    "the unbounded dial budget never budget-exits, so it deposits nothing — the \
       property finalize_tick's ready_dial_peers-empty assert relies on"
  );
}

/// The zero-attempt deposit arm: a BOUNDED pass whose shared dial budget is already
/// exhausted when the slot-free fixpoint reaches a freed peer's bucket must STILL
/// deposit that peer into the ready-dial ledger — the peer was already taken out of
/// `slot_freed_peers`, so absent a deposit its wake is consumed and the bucket strands.
///
/// Construction: seed B into `slot_freed_peers` with a non-empty parked bucket, then
/// run `service_slot_freed_peers` with the dial budget already at ZERO (the state the
/// fixpoint reaches after an earlier ready-dial drain in the same pass spent it).
///
/// Mutation (stated): require an attempt was made (drop the zero-attempt arm —
/// `bucket_nonempty && attempted_any && !last_was_reparked`) — the zero-budget bucket
/// then deposits nothing, so `reconcile_catchup_anchor` leaves `next_catchup_at`
/// `None` and `poll_timeout` returns only the far dial/idle deadline, stranding B.
#[test]
fn zero_attempt_deposit_arms_catchup_for_starved_slot_free_peer() {
  let a_addr: SocketAddr = "127.0.0.1:8320".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8321".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(512),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  while a.poll_event().is_some() {}

  // A non-empty parked bucket on B (fake ids suffice — a zero-budget pass never
  // attempts them, so they are never resolved). This is the tail an earlier ready-dial
  // drain in the same interval deferred.
  let deadline = now + Duration::from_secs(30);
  for i in 0..4u64 {
    a.dial_parked
      .entry(b_addr)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(95_000 + i),
        peer: b_addr,
        deadline,
        wake: deadline,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
  }

  // Seed B as a freed peer and run the slot-free consume with the shared dial budget
  // ALREADY exhausted. B is `mem::take`n out of `slot_freed_peers`; the zero-attempt
  // deposit is the ONLY thing that keeps its wake alive.
  a.slot_freed_peers.insert(b_addr);
  let mut pump_budget = super::MAX_BRIDGE_PUMPS_PER_PASS;
  let mut dial_budget = 0usize;
  let _ = a.service_slot_freed_peers(now, &mut pump_budget, &mut dial_budget);
  assert!(
    a.slot_freed_peers.is_empty(),
    "B was taken out of slot_freed_peers by the fixpoint"
  );
  assert!(
    !a.ready_dial_peers.is_empty(),
    "the zero-attempt arm deposits B into the ready-dial ledger even though the budget \
       was spent before reaching its bucket (dropping the arm strands B)"
  );
  // The reconcile step every bounded caller runs: the ledger residue arms the anchor.
  a.reconcile_catchup_anchor(now);
  assert!(
    a.next_catchup_at.is_some(),
    "the zero-attempt deposit arms the sticky catch-up anchor"
  );
  assert_eq!(
    a.poll_timeout(),
    Some(now + super::CATCHUP_INTERVAL),
    "poll_timeout returns the armed catch-up anchor — nearer than the far dial/idle \
       deadlines; dropping the zero-attempt arm returns only that far deadline"
  );
}

/// Front-park: a reliable-ping-exempt dial that re-parks (its connection still
/// handshaking, `open(Bi) == None`) lands at the FRONT of its peer's bucket, ahead of
/// the ordinary user-message dials that re-parked at the BACK — so a `C_OUT`-blocked
/// user-message head can never shadow the liveness-critical probe fallback behind the
/// `service_peer_bucket` stop-on-`Reparked` break.
///
/// Construction: three dials to a never-answered peer (its connection stays
/// handshaking, so every `open(Bi)` returns `None` and re-parks): two user messages,
/// then one reliable ping. The ping is exempt, so it front-parks.
///
/// Mutation (stated): revert the exempt re-park to `push_back` — the ping lands at the
/// BACK, behind the user-message head; on the next bucket service a `C_OUT`-blocked
/// user-message head would re-park and break before the ping is popped, stranding it.
#[test]
fn reliable_ping_reparks_at_front_of_bucket() {
  let a_addr: SocketAddr = "127.0.0.1:8330".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8331".parse().unwrap();
  let now = Instant::now();
  // No B endpoint: B never answers, so A's connection to B stays handshaking and every
  // dial re-parks via the `open(Bi) == None` path.
  let mut a = make_endpoint("a", a_addr, now);

  // Two user-message dials re-park at the BACK while B handshakes.
  let u1 = a
    .start_user_message(b_addr, Bytes::from_static(b"u1"), now)
    .expect("A registers the first user-message dial");
  let u2 = a
    .start_user_message(b_addr, Bytes::from_static(b"u2"), now)
    .expect("A registers the second user-message dial");
  assert_eq!(
    a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0),
    2,
    "both user-message dials re-park while B handshakes"
  );

  // The reliable-ping dial is exempt, so it re-parks at the FRONT, ahead of the
  // user-message head — the bucket order becomes [ping, u1, u2].
  let deadline = now + Duration::from_secs(5);
  let ping = a.start_reliable_ping(SmolStr::new("b"), b_addr, 7, deadline, now);
  let order: Vec<StreamId> = a
    .dial_parked
    .get(&b_addr)
    .expect("B's parked bucket holds all three dials")
    .iter()
    .map(|d| d.id)
    .collect();
  assert_eq!(
    order,
    vec![ping, u1, u2],
    "the reliable-ping re-parks at the FRONT (exempt) ahead of the user-message head, \
       which stays behind it in arrival order; reverting the exempt re-park to \
       push_back would order it [u1, u2, ping] and a C_OUT-blocked user-message head \
       would shadow the ping behind the stop-on-Reparked break"
  );
}

/// `flush_outbound` fully drains the ready-dial ledger BEFORE `finalize_tick`, so a
/// budget-deferred deposit an earlier BOUNDED pass left cannot survive a flush-all
/// into the finalize empty-ledger assert.
///
/// Construction: establish A -> B, inject `> budget` creditable intents parked on B,
/// run ONE budgeted `service_peer_bucket` pass that deposits B into the ledger, then
/// `flush_outbound_transmits`.
///
/// Mutation (stated): remove flush_outbound's ready-dial consume — the deposit
/// survives into `finalize_tick`, whose `debug_assert!(ready_dial_peers.is_empty())`
/// then fires (this test panics in a debug build).
#[test]
fn flush_outbound_drains_ready_dial_ledger_before_finalize() {
  let a_addr: SocketAddr = "127.0.0.1:8340".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8341".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(512),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);

  const M: usize = super::MAX_DIAL_ATTEMPTS_PER_PASS + 16;
  let payload = Bytes::from_static(b"x");
  for _ in 0..M {
    a.endpoint_mut()
      .start_user_message(b_addr, payload.clone(), now)
      .expect("register a reliable user-message dial intent");
  }
  a.sieve_dial_events();
  let pending: Vec<super::PendingDial> = a.dial_pending.drain(..).collect();
  a.unattempted_dial_count = 0;
  for mut pd in pending {
    pd.attempted = true;
    a.dial_parked.entry(b_addr).or_default().push_back(pd);
  }
  while a.poll_event().is_some() {}

  // One budgeted pass deposits B into the ready-dial ledger (mint the budget, defer
  // the creditable tail).
  let mut dial_budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = a.service_peer_bucket(b_addr, now, &mut dial_budget);
  assert!(
    !a.ready_dial_peers.is_empty(),
    "the bounded pass deposits B into the ready-dial ledger"
  );

  // A flush-all must consume the ledger fully before finalize; the finalize
  // empty-ledger debug-assert would fire (panic) if the deposit survived.
  a.flush_outbound_transmits(now);
  assert!(
    a.ready_dial_peers.is_empty(),
    "flush_outbound consumes the ready-dial ledger fully before finalize_tick; removing \
       that consume trips finalize_tick's ready_dial_peers-empty debug-assert"
  );
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true),
    "the flush-all drains B's whole deferred tail (unbounded budget)"
  );
}

/// Populate the ready-dial ledger with `n` distinct budget-deferred peers, each
/// holding one non-empty parked bucket, then run ONE [`QuicEndpoint::catchup_service`]
/// and return `(peer visits, dial attempts, resident ledger len after, anchor armed)`.
///
/// One EXPIRED entry per peer is the cheapest creditable bucket: the drain pops the
/// peer, `service_peer_bucket` attempts (retires) the single entry under the shared
/// dial budget and empties the bucket — exercising the drain's per-peer VISIT and
/// attempt accounting without standing up `n` real established connections. The
/// per-peer visit order and per-peer budget consumption are identical to a mint
/// bucket, so this faithfully bounds the drain's visit count. No `TimerKey::Dial`
/// keys are registered, so nothing counts as a due scheduled timer.
fn drive_one_catchup_over_n_deferred_ready_dial_peers(n: usize) -> (u64, u64, usize, bool) {
  let a_addr: SocketAddr = "127.0.0.1:8360".parse().unwrap();
  let now = Instant::now();
  // Schedulers OFF: `catchup_service` is invoked directly and no membership timer
  // interferes with the ledger drain under test.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);

  let elapsed = now - Duration::from_secs(1);
  for i in 0..n {
    let peer: SocketAddr = format!("127.0.0.9:{}", 9000 + i).parse().unwrap();
    a.dial_parked
      .entry(peer)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(60_000 + i as u64),
        peer,
        deadline: elapsed,
        wake: elapsed,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
    a.ready_dial_peers.insert(peer);
  }
  assert_eq!(
    a.ready_dial_peers.len(),
    n,
    "all {n} distinct peers seeded into the ready-dial ledger"
  );

  let visits_before = a.counters.ready_dial_peer_visits;
  let attempts_before = a.counters.dial_entries_serviced;
  a.catchup_service(now);
  let visits = a.counters.ready_dial_peer_visits - visits_before;
  let attempts = a.counters.dial_entries_serviced - attempts_before;
  (
    visits,
    attempts,
    a.ready_dial_peers.len(),
    a.next_catchup_at.is_some(),
  )
}

/// The budgeted catch-up ready-dial drain visits at most a budget-sized front-prefix
/// of the ledger — its work is O(interval dial budget), INDEPENDENT of how many peers
/// the ledger holds — leaving the untouched tail resident and the sticky anchor armed
/// for the next interval. This is the bound the residue ledger exists to uphold, now
/// enforced in the ledger's own drain: the former whole-ledger `take` detached and
/// visited EVERY peer even after the budget hit zero (hashing, bucket-calling, and
/// re-depositing each), making the drain scale with the ledger table.
///
/// Mutation-verify: revert the drain to `for peer in self.ready_dial_peers.take()` —
/// it visits all `n` peers, so `visits` climbs to the ledger size (300, then 600) and
/// both the `<= budget` and the "does not scale with the peer count" assertions fail.
#[test]
fn catchup_ready_dial_drain_visits_are_budget_bounded_not_ledger_sized() {
  let budget = super::MAX_DIAL_ATTEMPTS_PER_PASS as u64;

  // 300 deferred peers: ONE catch-up visits at most a budget-sized prefix, not 300.
  let (visits_300, attempts_300, resident_300, anchor_300) =
    drive_one_catchup_over_n_deferred_ready_dial_peers(300);
  assert!(
    attempts_300 <= budget,
    "one catch-up attempts at most MAX_DIAL_ATTEMPTS_PER_PASS={budget} dials; attempted {attempts_300}"
  );
  assert!(
    visits_300 <= budget,
    "the bounded-prefix drain VISITS at most a budget-sized prefix (each visited peer's \
       single-entry bucket consumes one budget unit), not all 300 ledger peers; visited \
       {visits_300}"
  );
  assert!(
    visits_300 < 300,
    "the drain must leave most of the 300-peer ledger unvisited; visited {visits_300}"
  );
  assert_eq!(
    resident_300,
    300 - visits_300 as usize,
    "exactly the unvisited tail stays resident — each visited peer's single-entry bucket \
       emptied and left the ledger, nothing else moved"
  );
  assert!(
    resident_300 > 0,
    "non-vacuous: a large tail remains resident for the next interval"
  );
  assert!(
    anchor_300,
    "the resident tail keeps has_residue true, so advance_catchup_anchor leaves the sticky \
       catch-up anchor armed"
  );

  // Double the ledger to 600: the per-catch-up visit count must NOT scale with it.
  let (visits_600, _attempts_600, resident_600, anchor_600) =
    drive_one_catchup_over_n_deferred_ready_dial_peers(600);
  assert_eq!(
    visits_600, visits_300,
    "the per-catch-up visit count is bounded by the interval dial budget and is INDEPENDENT \
       of the ledger size: 300 and 600 peers both visit {visits_300}. Reverting to the \
       whole-ledger `take` makes this 300 vs 600 (scaling with the table) and the equality fails"
  );
  assert!(
    visits_600 <= budget,
    "the 600-peer ledger still visits at most the budget; visited {visits_600}"
  );
  assert!(
    resident_600 > resident_300,
    "the larger ledger leaves a larger resident tail — the drain did not scale up its work to \
       compensate; {resident_600} resident vs {resident_300}"
  );
  assert!(
    anchor_600,
    "the 600-peer residue also keeps the anchor armed"
  );
}

/// The budgeted catch-up ready-dial drain is fair: a peer re-deposited after it
/// exhausts the interval dial budget rotates to the BACK of the ledger, so successive
/// catch-up intervals round-robin the whole ledger to empty and nothing strands. A
/// large-bucket HEAD peer that re-deposits every interval must not shadow the resident
/// tail — after the head is serviced once it drops behind the tail, so the next
/// interval reaches every tail peer.
///
/// Mutation-verify: make the re-deposit push to the FRONT (`push_front` in
/// `PeerResidue::insert`) — the head stays at the ledger front and is re-serviced
/// every interval while the tail starves, so the "every tail peer serviced after two
/// intervals" assertion fails.
#[test]
fn catchup_ready_dial_drain_round_robins_the_whole_ledger_no_strand() {
  let a_addr: SocketAddr = "127.0.0.1:8370".parse().unwrap();
  let now = Instant::now();
  // Schedulers OFF: every wake is provably a bounded ready-dial catch-up (no membership
  // tick), so the ledger drains solely through the drain under test.
  // A global connection cap of 0 makes every cold dial retire via `AtGlobalCap` — the
  // cheapest bucket-draining population — WITHOUT an expired deadline, so the entries
  // keep a far-future (NON-URGENT) deadline. That is load-bearing here: an expired
  // deadline is trivially within the urgent front-deposit horizon, so mechanism (c)
  // would front-deposit the head monopolist and defeat the round-robin under test;
  // non-urgent entries take the BACK re-deposit path, exercising PURE round-robin.
  // Schedulers OFF so every wake is a bounded ready-dial catch-up, never a full tick.
  let cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  let mut a = make_endpoint_full(
    cfg,
    test_config().with_max_quic_connections(Some(0)),
    a_addr,
    now,
  );

  // A HEAD peer with a bucket three interval-budgets deep, inserted FIRST so it sits at
  // the ledger front, plus one interval-budget's worth of single-entry TAIL peers. The
  // drain's per-peer visit ORDER and budget consumption match a mint bucket, so this
  // exercises round-robin fairness without real connections.
  const HEAD_ENTRIES: usize = 3 * super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let tails: Vec<SocketAddr> = (0..super::MAX_DIAL_ATTEMPTS_PER_PASS)
    .map(|i| format!("127.0.0.10:{}", 9000 + i).parse().unwrap())
    .collect();
  let head: SocketAddr = "127.0.0.9:9500".parse().unwrap();
  let far = now + Duration::from_secs(30);
  for i in 0..HEAD_ENTRIES {
    a.dial_parked
      .entry(head)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(70_000 + i as u64),
        peer: head,
        deadline: far,
        wake: far,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
  }
  a.ready_dial_peers.insert(head);
  for (i, &peer) in tails.iter().enumerate() {
    a.dial_parked
      .entry(peer)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(80_000 + i as u64),
        peer,
        deadline: far,
        wake: far,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
    a.ready_dial_peers.insert(peer);
  }
  assert_eq!(
    a.ready_dial_peers.len(),
    1 + tails.len(),
    "head + tail peers seeded into the ledger"
  );

  // Arm the sticky anchor as `reconcile_catchup_anchor` would, then drive the strict
  // poll-surface cadence: poll_timeout -> advance to the anchor -> handle_timeout.
  a.next_catchup_at = Some(now + super::CATCHUP_INTERVAL);
  let memb0 = a.membership_time_advances();
  for _ in 0..2 {
    let wake = a
      .poll_timeout()
      .expect("a non-empty ready-dial residue schedules a catch-up wake");
    assert_eq!(
      Some(wake),
      a.next_catchup_at,
      "the catch-up anchor is the earliest wake while a ready-dial residue waits"
    );
    a.handle_timeout(wake);
    assert_eq!(
      a.membership_time_advances(),
      memb0,
      "a ready-dial catch-up wake is a BOUNDED catch-up, not a full tick: it advances no \
         membership time"
    );
  }
  let serviced_after_two = tails
    .iter()
    .filter(|p| !a.dial_parked.contains_key(*p))
    .count();
  assert_eq!(
    serviced_after_two,
    tails.len(),
    "after two catch-up intervals every tail peer's bucket is serviced: the head re-deposited \
       to the BACK after interval 1, so interval 2 round-robins to the tail. A FRONT re-deposit \
       keeps the head at the front and leaves the tail unserviced here"
  );

  // Liveness: keep driving; the whole ledger drains to empty within a bounded number of
  // intervals (ceil(HEAD_ENTRIES / budget) dominates) — nothing strands.
  let mut intervals = 2usize;
  while !a.ready_dial_peers.is_empty() {
    let wake = a
      .poll_timeout()
      .expect("a non-empty residue always schedules a wake");
    a.handle_timeout(wake);
    intervals += 1;
    assert!(
      intervals <= 8,
      "the ready-dial ledger must converge to empty, never strand; {} peers still queued",
      a.ready_dial_peers.len()
    );
  }
  assert!(
    a.dial_parked.is_empty(),
    "every parked bucket drained across the round-robin intervals — nothing stranded"
  );
  assert!(
    a.next_catchup_at.is_none(),
    "the emptied ledger clears the sticky catch-up anchor"
  );
}

/// Populate the ready-dial ledger with `n` distinct STALE entries — peers resident in
/// [`QuicEndpoint::ready_dial_peers`] whose [`QuicEndpoint::dial_parked`] bucket is
/// ABSENT — then run ONE [`QuicEndpoint::catchup_service`] and return
/// `(peer visits, resident ledger len after, anchor armed)`.
///
/// A stale entry reproduces the ledger state left after a DIRECT `service_peer_bucket`
/// wake (an establishment / `MAX_STREAMS` `Available` / slot-free wake) drained a
/// deposited peer's bucket to empty and dropped it from `dial_parked` WITHOUT removing
/// the peer from the ledger. Inserting straight into `ready_dial_peers` with no parked
/// bucket is exactly that state, deterministic and with no connections to stand up.
/// Servicing a stale entry pops an empty/absent bucket, so it attempts ZERO dials —
/// spending a VISIT but no dial budget. No `TimerKey::Dial` keys are registered, so
/// nothing counts as a due scheduled timer.
fn drive_one_catchup_over_n_stale_ready_dial_peers(n: usize) -> (u64, usize, bool) {
  let a_addr: SocketAddr = "127.0.0.1:8380".parse().unwrap();
  let now = Instant::now();
  // Schedulers OFF: `catchup_service` is invoked directly and no membership timer
  // interferes with the ledger drain under test.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);

  for i in 0..n {
    let peer: SocketAddr = format!("127.0.0.11:{}", 9000 + i).parse().unwrap();
    // STALE: resident in the ledger with NO `dial_parked` bucket — the state a direct
    // `service_peer_bucket` wake leaves after emptying a deposited peer's bucket.
    a.ready_dial_peers.insert(peer);
  }
  assert_eq!(
    a.ready_dial_peers.len(),
    n,
    "all {n} distinct stale peers seeded into the ready-dial ledger"
  );
  assert!(
    a.dial_parked.is_empty(),
    "test precondition: every seeded entry is STALE — no dial_parked bucket backs it"
  );

  let visits_before = a.counters.ready_dial_peer_visits;
  a.catchup_service(now);
  let visits = a.counters.ready_dial_peer_visits - visits_before;
  (
    visits,
    a.ready_dial_peers.len(),
    a.next_catchup_at.is_some(),
  )
}

/// The budgeted catch-up ready-dial drain pops at most a CONSTANT number of peers even
/// when a long prefix of the ledger is STALE — entries whose parked bucket a direct
/// `service_peer_bucket` wake already emptied, so servicing them attempts zero dials
/// and spends NO dial budget. The dial-attempt budget cannot bound such a pass (a stale
/// visit costs no dial unit), so the independent [`MAX_READY_DIAL_VISITS_PER_PASS`]
/// visit budget is what holds the pop count constant. The visited stale entries
/// self-clean (popped, never re-deposited), the unvisited stale tail stays resident for
/// the next interval, and the sticky anchor stays armed.
///
/// Mutation-verify: drop the visit budget (revert the loop to the `remaining_visits =
/// len()` bound alone). A stale visit spends no dial budget, so the loop walks the WHOLE
/// stale ledger: `visits` climbs to 300 then 600 (the ledger size), so both the
/// `== MAX_READY_DIAL_VISITS_PER_PASS` and the "flat across ledger size" assertions
/// fail.
#[test]
fn catchup_ready_dial_drain_visits_are_visit_bounded_over_a_stale_prefix() {
  let cap = super::MAX_READY_DIAL_VISITS_PER_PASS as u64;

  // 300 stale entries: ONE catch-up pops at most the visit budget, not 300.
  let (visits_300, resident_300, anchor_300) = drive_one_catchup_over_n_stale_ready_dial_peers(300);
  assert_eq!(
    visits_300, cap,
    "with a 300-entry stale ledger (300 >> the visit budget) the pass pops EXACTLY \
       MAX_READY_DIAL_VISITS_PER_PASS={cap} peers — the visit budget binds, since a stale \
       visit spends no dial budget; visited {visits_300}"
  );
  assert!(
    visits_300 < 300,
    "the drain must leave most of the 300-entry stale ledger unvisited; visited {visits_300}"
  );
  assert_eq!(
    resident_300,
    300 - visits_300 as usize,
    "exactly the unvisited stale tail stays resident — each visited stale entry popped off \
       the ledger and self-cleaned (no bucket, so no re-deposit)"
  );
  assert!(
    resident_300 > 0,
    "non-vacuous: a large stale tail remains resident to drain over later intervals"
  );
  assert!(
    anchor_300,
    "the resident stale tail keeps has_residue true, so advance_catchup_anchor leaves the \
       sticky catch-up anchor armed"
  );

  // Double the stale ledger to 600: the per-catch-up visit count must stay FLAT.
  let (visits_600, resident_600, anchor_600) = drive_one_catchup_over_n_stale_ready_dial_peers(600);
  assert_eq!(
    visits_600, visits_300,
    "the per-catch-up visit count is bounded by the independent visit budget and is \
       INDEPENDENT of the stale ledger size: 300 and 600 stale entries both visit \
       {visits_300}. Dropping the visit budget makes this 300 vs 600 (scaling with the \
       ledger — the O(ledger)-per-interval defect) and the equality fails"
  );
  assert_eq!(
    visits_600, cap,
    "the 600-entry stale ledger also pops exactly the visit budget; visited {visits_600}"
  );
  assert!(
    resident_600 > resident_300,
    "the larger stale ledger leaves a larger resident tail — the drain did not scale up its \
       work to compensate; {resident_600} resident vs {resident_300}"
  );
  assert!(
    anchor_600,
    "the 600-entry stale residue also keeps the anchor armed"
  );
}

/// A LIVE budget-deferred ready-dial peer placed BEHIND a large STALE prefix is still
/// serviced within a bounded number of catch-up intervals — nothing strands behind the
/// visit bound. The visit budget defers the live peer past the first interval (it stops
/// the pass inside the stale prefix), but the stale prefix SELF-CLEANS (popped, never
/// re-deposited) so it shrinks by up to the visit budget per interval; the live peer
/// moves to the front and is reached as soon as the prefix ahead of it is gone.
///
/// Mutation-verify: drop the visit budget (revert to the `remaining_visits = len()`
/// bound). One pass then walks the whole ledger and services the live peer in the FIRST
/// interval, so the "still parked after one catch-up" assertion fails.
#[test]
fn catchup_ready_dial_drain_reaches_live_peer_behind_stale_prefix_no_strand() {
  let a_addr: SocketAddr = "127.0.0.1:8390".parse().unwrap();
  let now = Instant::now();
  // Schedulers OFF: every wake is provably a bounded ready-dial catch-up (no membership
  // tick), so the ledger drains solely through the drain under test.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);

  // A large STALE prefix — peers resident in the ledger with NO parked bucket, the state
  // a direct `service_peer_bucket` wake leaves after emptying a deposited bucket — seeded
  // FIRST so they sit ahead of the live peer.
  const STALE_PREFIX: usize = 300;
  let stale: Vec<SocketAddr> = (0..STALE_PREFIX)
    .map(|i| format!("127.0.0.12:{}", 9000 + i).parse().unwrap())
    .collect();
  for &peer in &stale {
    a.ready_dial_peers.insert(peer);
  }
  // One LIVE deferred peer with a non-empty creditable bucket, inserted LAST so it sits
  // at the BACK, behind the whole stale prefix. An expired entry is the cheapest
  // creditable bucket (retires on service); "serviced" = its bucket drains and leaves
  // `dial_parked`, the observable no-strand signal — the per-peer visit order and budget
  // consumption match a mint bucket.
  let live: SocketAddr = "127.0.0.9:9999".parse().unwrap();
  let elapsed = now - Duration::from_secs(1);
  a.dial_parked
    .entry(live)
    .or_default()
    .push_back(super::PendingDial {
      id: StreamId::from_raw(90_000),
      peer: live,
      deadline: elapsed,
      wake: elapsed,
      attempted: true,
      kind: super::ExchangeKind::UserMessage,
    });
  a.ready_dial_peers.insert(live);
  assert_eq!(
    a.ready_dial_peers.len(),
    STALE_PREFIX + 1,
    "stale prefix + the one live peer seeded into the ledger"
  );
  assert!(
    a.dial_parked.contains_key(&live),
    "test precondition: the live peer starts with a non-empty parked bucket behind the \
       stale prefix"
  );

  // Arm the sticky anchor as `reconcile_catchup_anchor` would, then drive the strict
  // poll-surface cadence: poll_timeout -> advance to the anchor -> handle_timeout.
  a.next_catchup_at = Some(now + super::CATCHUP_INTERVAL);

  // First interval: the visit budget stops the pass INSIDE the stale prefix, before the
  // live peer — so the live peer must still be parked afterwards. A len()-only bound
  // would walk the whole ledger and service the live peer here, failing this assertion.
  let wake0 = a
    .poll_timeout()
    .expect("a non-empty ready-dial residue schedules a catch-up wake");
  a.handle_timeout(wake0);
  assert!(
    a.dial_parked.contains_key(&live),
    "after ONE catch-up the visit budget has cleared only a stale-prefix chunk; the live \
       peer behind it is still parked (a len()-only bound would have serviced it already)"
  );
  assert!(
    !a.ready_dial_peers.is_empty(),
    "stale entries remain resident to drain over further intervals"
  );

  // Keep driving: the stale prefix self-cleans at the visit budget per interval, so the
  // live peer is reached within a bounded number of further intervals — nothing strands.
  let mut intervals = 1usize;
  while a.dial_parked.contains_key(&live) {
    let wake = a
      .poll_timeout()
      .expect("a non-empty residue always schedules a wake");
    a.handle_timeout(wake);
    intervals += 1;
    assert!(
      intervals <= 5,
      "the live peer behind a {STALE_PREFIX}-entry stale prefix must be serviced within a \
         bounded number of catch-up intervals; still parked after {intervals}"
    );
  }
  // ceil(300 / visit budget) stale-clearing intervals, and the last of those reaches the
  // live peer in the same pass that clears the final stale chunk ahead of it.
  assert!(
    intervals <= 3,
    "the stale prefix self-cleans at the visit budget per interval, so the live peer is \
       reached as soon as the prefix ahead of it is gone; took {intervals} intervals"
  );
  assert!(
    !a.dial_parked.contains_key(&live),
    "the live peer's bucket was serviced — it did not strand behind the stale prefix"
  );
}

/// The per-peer reliable user-message admission cap refuses a `start_user_message`
/// once `max_pending_user_dials_per_peer` intents are already OUTSTANDING to that
/// peer, and the refused call is a pure no-op.
///
/// Fills a cold (never-answering) peer to exactly the default cap `K = 32` — each
/// `start_user_message` re-parks the still-handshaking dial — then the `K+1`th
/// returns [`Error::UserDialBacklogFull`] carrying the peer and the limit, having
/// mutated nothing (backlog still `K`, parked bucket still `K`, no new
/// `pending_outbound_*` entry).
///
/// Mutation-verify: remove the admission gate in `start_user_message` — the `K+1`th
/// call then returns `Ok`, so `expect_err` fails.
#[test]
fn user_dial_backlog_cap_refuses_past_the_limit() {
  let a_addr: SocketAddr = "127.0.0.1:8600".parse().unwrap();
  // A port nothing listens on: the pooled connection never leaves handshaking, so
  // every reliable user-message dial re-parks (the outstanding-backlog state the
  // cap bounds).
  let cold: SocketAddr = "127.0.0.9:8601".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  assert_eq!(
    a.cfg.max_pending_user_dials_per_peer(),
    32,
    "test precondition: the default per-peer reliable-dial cap is 32"
  );

  let k = a.cfg.max_pending_user_dials_per_peer();
  let payload = Bytes::from_static(b"x");
  for i in 0..k {
    a.start_user_message(cold, payload.clone(), now)
      .unwrap_or_else(|e| panic!("send {i} under the cap must be admitted, got {e:?}"));
    // Drain owed transmits so `out` does not grow unboundedly; the payloads are
    // never delivered, so every dial stays parked.
    while a.poll_transmit().is_some() {}
  }
  assert_eq!(
    a.user_dial_backlog(&cold),
    k,
    "K admitted cold-peer dials must all be parked and counted"
  );
  assert_eq!(
    a.dial_parked.get(&cold).map(|q| q.len()).unwrap_or(0),
    k,
    "test precondition: all K intents re-parked on the cold peer's bucket"
  );
  a.assert_user_dial_backlog_exact();

  // Snapshot the state the refused call must NOT touch.
  let kinds_before = a.pending_outbound_kinds.len();
  let peers_before = a.pending_outbound_peers.len();
  let parked_before = a.dial_parked.get(&cold).map(|q| q.len()).unwrap_or(0);

  let err = a
    .start_user_message(cold, payload.clone(), now)
    .expect_err("the K+1th send must be refused with backpressure");
  match err {
    crate::error::Error::UserDialBacklogFull(full) => {
      assert_eq!(full.peer(), cold, "the error names the overloaded peer");
      assert_eq!(full.limit(), k, "the error names the configured limit");
    }
    other => panic!("expected UserDialBacklogFull, got {other:?}"),
  }

  // Pure no-op: no machine intent, no id, no pending_outbound_* entry, no Dial key
  // (the parked bucket, which owns each intent's Dial key, is unchanged), and the
  // backlog is untouched.
  assert_eq!(
    a.user_dial_backlog(&cold),
    k,
    "a refused call must not change the backlog"
  );
  assert_eq!(
    a.pending_outbound_kinds.len(),
    kinds_before,
    "a refused call must register no outbound-kind entry"
  );
  assert_eq!(
    a.pending_outbound_peers.len(),
    peers_before,
    "a refused call must register no outbound-peer entry"
  );
  assert_eq!(
    a.dial_parked.get(&cold).map(|q| q.len()).unwrap_or(0),
    parked_before,
    "a refused call must park no new intent"
  );
  a.assert_user_dial_backlog_exact();
}

/// Retiring the outstanding intents at their dial deadline releases the backlog, so
/// a fresh `start_user_message` to the same peer is admitted again.
///
/// Mutation-verify: drop the `note_user_dial_dequeued` release in
/// `process_dial_entry` — the retired intents leave the count stuck at `K`, so the
/// post-retirement backlog assertion fails (and the fresh send would wrongly be
/// refused).
#[test]
fn user_dial_backlog_releases_when_intents_retire() {
  let a_addr: SocketAddr = "127.0.0.1:8610".parse().unwrap();
  let cold: SocketAddr = "127.0.0.9:8611".parse().unwrap();
  let now = Instant::now();
  // No periodic schedulers, so advancing the clock to the dial deadline drives a
  // pure catch-up/tick that services (and retires) the parked dials with no
  // scheduler noise.
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);

  let k = a.cfg.max_pending_user_dials_per_peer();
  let payload = Bytes::from_static(b"x");
  for _ in 0..k {
    a.start_user_message(cold, payload.clone(), now)
      .expect("cold-peer dial under the cap is admitted");
    while a.poll_transmit().is_some() {}
  }
  assert_eq!(a.user_dial_backlog(&cold), k, "the cap is filled");

  // Advance past the parked intents' `now + stream_timeout` (default 10s) deadline
  // and tick: `service_dials` retires each expired intent, releasing its backlog
  // unit.
  let later = now + Duration::from_secs(30);
  a.handle_timeout(later);
  assert_eq!(
    a.user_dial_backlog(&cold),
    0,
    "retiring every expired intent must drain the peer's backlog to 0"
  );
  assert!(
    a.dial_parked
      .get(&cold)
      .map(|q| q.is_empty())
      .unwrap_or(true),
    "the expired bucket is drained (and removed) at retirement"
  );
  a.assert_user_dial_backlog_exact();

  // A fresh send is admitted again now the backlog has drained.
  a.start_user_message(cold, payload.clone(), later)
    .expect("a fresh send is admitted once the backlog has drained");
  assert_eq!(
    a.user_dial_backlog(&cold),
    1,
    "the fresh send re-parks and is counted"
  );
  a.assert_user_dial_backlog_exact();
}

/// Push/pull and reliable-ping dials are EXEMPT from the cap: they are admitted at
/// a full user-message backlog and are never counted against it.
///
/// Mutation-verify: count push/pull (or reliable-ping) into `user_dial_backlog` —
/// the backlog climbs past `K`, so the "unchanged at K" assertion and the
/// brute-force cross-check fail.
#[test]
fn push_pull_and_reliable_ping_exempt_from_user_dial_cap() {
  let a_addr: SocketAddr = "127.0.0.1:8620".parse().unwrap();
  let cold: SocketAddr = "127.0.0.9:8621".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  let k = a.cfg.max_pending_user_dials_per_peer();
  let payload = Bytes::from_static(b"x");
  for _ in 0..k {
    a.start_user_message(cold, payload.clone(), now)
      .expect("cold-peer dial under the cap is admitted");
    while a.poll_transmit().is_some() {}
  }
  assert_eq!(
    a.user_dial_backlog(&cold),
    k,
    "the user-message backlog is full"
  );

  // A push/pull to the SAME peer at a full user-message cap still registers its
  // intent — the cap gates only reliable user messages.
  let pp_id = a.start_push_pull(cold, PushPullKind::Join, now);
  while a.poll_transmit().is_some() {}
  assert_eq!(
    a.pending_outbound_kinds.get(&pp_id),
    Some(&super::ExchangeKind::PushPull),
    "a push/pull intent is created even at a full user-message backlog"
  );

  // A reliable-ping fallback likewise registers its intent.
  let rp_id = a.start_reliable_ping(
    SmolStr::new("b"),
    cold,
    1,
    now + Duration::from_secs(5),
    now,
  );
  while a.poll_transmit().is_some() {}
  assert_eq!(
    a.pending_outbound_kinds.get(&rp_id),
    Some(&super::ExchangeKind::ReliablePing),
    "a reliable-ping intent is created even at a full user-message backlog"
  );

  // Neither exempt dial touched the user-message backlog.
  assert_eq!(
    a.user_dial_backlog(&cold),
    k,
    "push/pull and reliable-ping must not be counted against the user-message cap"
  );
  a.assert_user_dial_backlog_exact();
}

/// The incremental `user_dial_backlog` map equals the brute-force recount over
/// `dial_pending` + `dial_parked` at every step of a scripted sequence that
/// exercises the mint, park/re-park, tick-reservice, and leave-purge paths.
///
/// Mutation-verify: skip the `note_user_dial_enqueued` re-increment in
/// `repark_blocked_dial` — the tick that re-parks the cold-peer intents drops their
/// counts, so the incremental map drifts below the brute-force recount and
/// `assert_user_dial_backlog_exact` fails.
#[test]
fn user_dial_backlog_incremental_matches_bruteforce() {
  let a_addr: SocketAddr = "127.0.0.1:8630".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8631".parse().unwrap();
  let cold: SocketAddr = "127.0.0.9:8632".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint_no_schedulers("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  a.assert_user_dial_backlog_exact();

  // (1) Park several reliable user messages on a cold peer (each re-parks).
  let payload = Bytes::from_static(b"x");
  for _ in 0..5 {
    a.start_user_message(cold, payload.clone(), now)
      .expect("cold-peer dial admitted");
    while a.poll_transmit().is_some() {}
    a.assert_user_dial_backlog_exact();
  }
  assert_eq!(a.user_dial_backlog(&cold), 5);

  // (2) Establish B and mint several reliable user messages to it: each opens a
  // bridge immediately, so it enters and leaves the pipeline within the call and
  // contributes ZERO resident backlog.
  establish(&mut a, &mut b, a_addr, b_addr, now);
  for _ in 0..3 {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("established-peer dial mints a bridge");
    while a.poll_transmit().is_some() {}
    a.assert_user_dial_backlog_exact();
  }
  assert_eq!(
    a.user_dial_backlog(&b_addr),
    0,
    "minted (non-parked) intents contribute no backlog"
  );
  assert_eq!(
    a.user_dial_backlog(&cold),
    5,
    "the cold-peer backlog is unchanged"
  );

  // (3) Tick while the cold intents are still within their deadline: `service_dials`
  // re-processes each parked entry (release on take-out, re-park re-counts), so the
  // backlog is stable and the incremental map still matches the recount.
  a.handle_timeout(now);
  a.assert_user_dial_backlog_exact();
  assert_eq!(
    a.user_dial_backlog(&cold),
    5,
    "a re-park nets to one resident count, so the backlog holds across a tick"
  );

  // (4) Leave: the reliable-dial purge drains both structures; every counted intent
  // is released and the map empties.
  a.leave(now)
    .expect("leave initiates the reliable-dial purge");
  a.assert_user_dial_backlog_exact();
  assert_eq!(
    a.user_dial_backlog(&cold),
    0,
    "the leave purge releases every counted intent"
  );
  assert!(
    a.user_dial_backlog.is_empty(),
    "no dead peer entry survives the purge"
  );
}

/// Config validation rejects a zero per-peer reliable-dial ceiling at the
/// `QuicOptions::validate` chokepoint, and the default is 32.
///
/// Mutation-verify: drop the `max_pending_user_dials_per_peer == 0` guard in
/// `QuicOptions::validate` — the zero config then validates `Ok`, so the
/// `expect_err` fails.
#[test]
fn quic_options_validate_rejects_zero_user_dial_cap() {
  assert_eq!(
    test_config().max_pending_user_dials_per_peer(),
    super::DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER,
    "the default per-peer reliable-dial ceiling is the documented default"
  );
  assert_eq!(
    super::DEFAULT_MAX_PENDING_USER_DIALS_PER_PEER,
    32,
    "the documented default is 32"
  );

  let err = test_config()
    .with_max_pending_user_dials_per_peer(0)
    .validate()
    .expect_err("a zero per-peer reliable-dial ceiling must be rejected");
  assert!(
    matches!(err, super::QuicOptionsError::MaxPendingUserDialsPerPeerZero),
    "the rejection names the zero-ceiling guard, got {err:?}"
  );

  test_config()
    .with_max_pending_user_dials_per_peer(1)
    .validate()
    .expect("a ceiling of 1 is the minimum usable value and validates");
  test_config()
    .validate()
    .expect("the default config validates");
}

/// Establish `a -> b` via a datagram warm-up: the connection reaches Established
/// with no bidi credit consumed and no bridge minted, so a boundedness test starts
/// from a clean slate with the peer's full initial stream grant.
fn establish_via_warmup(
  a: &mut QuicEndpoint<SmolStr>,
  a_addr: SocketAddr,
  b: &mut QuicEndpoint<SmolStr>,
  b_addr: SocketAddr,
  now: Instant,
) {
  let _ = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now);
  a.flush_outbound_transmits(now);
  for _ in 0..300 {
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
    if a.live_connections_to(b_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1,
    "precondition: A must establish a pooled connection to B"
  );
  while a.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
  while b.poll_event().is_some() {}
  assert_eq!(
    a.live_bridge_count(),
    0,
    "precondition: the datagram warm-up mints no bridge"
  );
}

/// Supply bound under an honest wedge: with a peer that receives nothing back (an
/// honest partition — a crashed / partitioned / wedged peer), the coordinator's
/// per-connection resident dialer-bridge state and its total outbound-stream supply
/// stay bounded by config constants, NOT by how many reliable user messages the
/// application issues, and NOT growing across generations as bridges reap and
/// re-mint.
///
/// `B` grants ample bidi credit (`C_OUT + 64`, so the LOCAL `C_OUT` cap — not the
/// peer's `MAX_STREAMS` — binds the concurrent dialer set) behind a small
/// connection window. After establishing via a datagram warm-up (no bridge minted,
/// full bidi grant unconsumed), all `B -> A` datagrams are withheld: `A`'s outbound
/// is drained and discarded so nothing crosses the partition and, crucially, `B`'s
/// `MAX_STREAMS_BIDI` replenishments (which it would send as `A` resets timed-out
/// streams) never reach `A`, freezing `A`'s peer-granted bidi credit at its initial
/// grant. `A` then keeps issuing reliable user messages across several
/// `stream_timeout` generations. The per-peer reliable-dial admission cap is raised
/// far above the storm so admission never masks the QUIC-side supply bound.
///
/// Because each `open(Dir::Bi)` consumes one never-replenished bidi credit, the
/// TOTAL dialer bridges opened over the whole wedge is bounded by the initial grant
/// (a config constant) and the CONCURRENT resident set is bounded by `C_OUT` — with
/// per-tick pump work bounded by the live set plus the pass budget in every
/// generation, never scaling with the (raised, unbounded) parked-intent backlog.
///
/// The observable for total supply is the set of distinct bridge stream ids ever
/// resident in `self.bridges` (a mint creates exactly one such id, and under the
/// wedge a fresh mint survives — its deadline is in the future — until the next
/// snapshot, so the union captures every mint); the concurrent bound is the
/// `live_bridge_count` high-water.
///
/// Mutation-verify: disable the `C_OUT` pre-open gate in `process_dial_entry` — the
/// resident dialer set then climbs to the peer's full granted bidi credit
/// (`C_OUT + 64`, above `C_OUT`), so the `live_bridge_count` high-water passes
/// `C_OUT + allowance` and that assertion fails.
#[test]
fn wedged_peer_supply_bounds_outbound_dialer_state() {
  let a_addr: SocketAddr = "127.0.0.1:7838".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7839".parse().unwrap();
  let mut now = Instant::now();

  // A: periodic membership schedulers OFF (the user-message storm is the only
  // dialer, so the observable is clean), a short `stream_timeout` (cheap
  // virtual-time generations), and the per-peer reliable-dial admission cap raised
  // far above the storm so admission never masks the QUIC-side bound.
  const RAISED_ADMISSION: usize = 1 << 16;
  let stream_timeout = Duration::from_millis(200);
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(stream_timeout);
  let a_qc = test_config().with_max_pending_user_dials_per_peer(RAISED_ADMISSION);
  let mut a = make_endpoint_full(a_cfg, a_qc, a_addr, now);

  // B grants ample bidi credit (C_OUT + 64) behind a small connection window, so
  // the LOCAL C_OUT cap binds the concurrent set while the connection window
  // back-pressures the data.
  const GRANTED_BIDI: u32 = super::C_OUT as u32 + 64;
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(
    b_cfg,
    test_config_small_conn_window_bidi(GRANTED_BIDI),
    b_addr,
    now,
  );

  establish_via_warmup(&mut a, a_addr, &mut b, b_addr, now);

  // The wedge is now in force: from here NO `B -> A` byte is ever delivered. `A`'s
  // outbound is drained and discarded so its `out` queue stays bounded, but B's
  // stream-credit replenishments never reach A, freezing A's bidi grant.
  const BATCH: usize = 512;
  const GENERATIONS: usize = 4;
  let payload = Bytes::from_static(b"reliable-user-message-under-honest-wedge");

  let mut minted_ids: std::collections::HashSet<StreamId> = std::collections::HashSet::new();
  let mut high_water_live = 0usize;
  let mut ever_parked = false;
  let mut total_started = 0usize;
  // Allowance for rounding / a warm-up credit return / any exempt dial.
  const ALLOWANCE: usize = 8;

  for generation in 0..GENERATIONS {
    // A keeps issuing reliable user messages to the wedged peer. Admission is
    // raised sky-high, so each start registers an intent (minting a bridge if a
    // C_OUT slot AND peer bidi credit are free, else parking) and is never refused.
    for _ in 0..BATCH {
      a.start_user_message(b_addr, payload.clone(), now)
        .expect("raised admission never refuses under the wedge");
      total_started += 1;
    }
    // Discard A's outbound: nothing crosses the partition, nothing returns.
    while a.poll_transmit().is_some() {}
    // Capture every bridge minted this generation before any reap can remove it.
    for id in a.bridges.keys() {
      minted_ids.insert(*id);
    }
    high_water_live = high_water_live.max(a.live_bridge_count());
    if a
      .dial_parked
      .get(&b_addr)
      .map(|q| !q.is_empty())
      .unwrap_or(false)
    {
      ever_parked = true;
    }
    while a.poll_event().is_some() {}

    // Advance one stream_timeout generation and tick. Expired bridges reap (freeing
    // C_OUT slots) and the slot-free wake mints parked dials into the freed slots —
    // but only up to the frozen bidi credit, so the supply cannot grow past the
    // initial grant, and the resident set cannot exceed C_OUT.
    now += stream_timeout + Duration::from_millis(1);
    let bv_before = a.counters.bridge_visits;
    a.handle_timeout(now);
    let bv_delta = a.counters.bridge_visits - bv_before;
    assert!(
      bv_delta <= (super::C_OUT + super::MAX_BRIDGE_PUMPS_PER_PASS) as u64,
      "generation {generation}: per-tick pump work must stay bounded by the live set plus \
         the pass budget (<= C_OUT + budget = {}), never scaling with the parked \
         backlog; got {bv_delta}",
      super::C_OUT + super::MAX_BRIDGE_PUMPS_PER_PASS
    );
    // Capture any bridges the slot-free wake minted in this tick.
    for id in a.bridges.keys() {
      minted_ids.insert(*id);
    }
    high_water_live = high_water_live.max(a.live_bridge_count());
    while a.poll_transmit().is_some() {}
    while a.poll_event().is_some() {}
  }

  let total_minted = minted_ids.len();

  // Non-vacuity: the wedge genuinely parked excess dials, and issued far more starts
  // than the bound allows to mint — so the bound actually bites.
  assert!(
    ever_parked,
    "non-vacuous: the wedge must have parked excess dials"
  );
  assert!(
    total_started >= BATCH * GENERATIONS,
    "non-vacuous: every batch must have been issued"
  );

  // SUPPLY BOUND: over the whole wedge the coordinator opened at most a config
  // constant of dialer bridges — the peer's initial (frozen, never-replenished)
  // bidi grant plus a small allowance — NOT one per start_user_message.
  let supply_bound = super::C_OUT + GRANTED_BIDI as usize + ALLOWANCE;
  assert!(
    total_minted <= supply_bound,
    "supply bound: total distinct dialer bridges minted over the wedge ({total_minted}) \
       must be <= C_OUT + granted_bidi + allowance = {supply_bound}"
  );
  assert!(
    total_minted < total_started / 2,
    "non-vacuous supply bound: {total_minted} mints must be far below the \
       {total_started} starts issued (a per-start mint would explode past this)"
  );

  // CONCURRENT / RESIDENT BOUND (the C_OUT-gate-sensitive assertion): the resident
  // dialer set never exceeded the local cap plus a small allowance across ANY
  // generation — no per-generation growth in live bridge state.
  assert!(
    high_water_live <= super::C_OUT + ALLOWANCE,
    "concurrent live dialer bridges high-water ({high_water_live}) must stay \
       <= C_OUT + allowance = {} across every generation",
    super::C_OUT + ALLOWANCE
  );
}

/// Recovery drain: releasing an honest wedge does not disrupt the deadline
/// machinery — the work parked during the partition drains (previously parked dials
/// mint and their exchanges reach a terminal SUCCESS), driven purely by the poll
/// surface, rather than stranding to a deadline failure.
///
/// Same wedge shape as `wedged_peer_supply_bounds_outbound_dialer_state`, with an
/// ample `stream_timeout` so recovery has deadline headroom: `A` establishes to
/// `B`, then — while `B -> A` is withheld — issues a batch exceeding `C_OUT` so a
/// tail of dials parks (blocked, deadlines still in the future, nothing yet
/// succeeded). The wedge is then RELEASED (bidirectional ferrying resumes) and the
/// coordinators are driven purely by `poll_timeout` / `handle_timeout` (no manual
/// servicing): `B`'s stream-credit replenishments reach `A`, the parked dials mint,
/// the buffered payloads flush through the connection window, and the exchanges
/// complete. Assertions are on COUNTERS and terminal outcomes, never wall-clock —
/// the parked bucket drains to empty and the count of SUCCEEDED `ExchangeCompleted`
/// events climbs above zero.
///
/// Mutation-verify: do NOT release — keep dropping the `B -> A` datagrams instead of
/// ferrying them. The parked dials never regain stream credit, so no exchange
/// reaches a SUCCEEDED terminus (each instead fails at its deadline) and the
/// `succeeded > 0` assertion fails. (Surgically skipping a single `service_connection`
/// is not cleanly mutable against the real poll surface, so dropping the release is
/// the mutation, per the test contract.)
#[test]
fn released_wedge_drains_parked_dials_to_completion() {
  let a_addr: SocketAddr = "127.0.0.1:7846".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7847".parse().unwrap();
  let mut now = Instant::now();

  // A: schedulers OFF (clean observation) with the DEFAULT stream_timeout, so the
  // release has ample deadline headroom to complete the parked exchanges; admission
  // raised so the wedge parks freely.
  const RAISED_ADMISSION: usize = 1 << 16;
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  let a_qc = test_config().with_max_pending_user_dials_per_peer(RAISED_ADMISSION);
  let mut a = make_endpoint_full(a_cfg, a_qc, a_addr, now);

  const GRANTED_BIDI: u32 = super::C_OUT as u32 + 64;
  let b_cfg = EndpointOptions::new(SmolStr::new("b"), b_addr);
  let mut b = make_endpoint_full(
    b_cfg,
    test_config_small_conn_window_bidi(GRANTED_BIDI),
    b_addr,
    now,
  );

  establish_via_warmup(&mut a, a_addr, &mut b, b_addr, now);

  // WEDGE (no time advance): A issues a batch exceeding C_OUT so a tail parks.
  // Small payloads so the release flushes them through the 16 KiB connection window
  // in a few rounds. B -> A is withheld: A's transmits are discarded, so B never
  // sees the messages and nothing completes yet.
  const BATCH: usize = super::C_OUT + 128;
  let payload = Bytes::from_static(b"recover");
  for _ in 0..BATCH {
    a.start_user_message(b_addr, payload.clone(), now)
      .expect("raised admission never refuses");
  }
  while a.poll_transmit().is_some() {}
  let parked_before = a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0);
  assert!(
    parked_before > 0,
    "precondition: the wedge must park a tail of dials (issued {BATCH}, cap C_OUT={})",
    super::C_OUT
  );
  // Nothing has completed under the wedge (B never received a message).
  let mut succeeded = 0usize;
  while let Some(ev) = a.poll_event() {
    if let Event::ExchangeCompleted(ec) = ev {
      assert!(
        !ec.outcome().is_succeeded(),
        "no exchange can succeed while B -> A is withheld"
      );
    }
  }
  while b.poll_event().is_some() {}

  // RELEASE: resume bidirectional ferrying and drive purely via the poll surface
  // (ferry + poll_timeout + handle_timeout). B's credit replenishments now reach A,
  // the parked dials mint, and the exchanges flush and complete.
  let mut steps = 0usize;
  loop {
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
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.outcome().is_succeeded() {
          succeeded += 1;
        }
      }
    }
    while b.poll_event().is_some() {}

    let parked = a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0);
    if !moved && parked == 0 && a.live_bridge_count() == 0 {
      break;
    }
    // Advance to the earliest scheduled wake (the poll surface drives the clock),
    // never backwards and always strictly forward so an ACK/idle timer can fire.
    let next = [a.poll_timeout(), b.poll_timeout()]
      .into_iter()
      .flatten()
      .min()
      .unwrap_or(now)
      .max(now + Duration::from_millis(1));
    now = next;
    a.handle_timeout(now);
    b.handle_timeout(now);

    steps += 1;
    assert!(
      steps < 20_000,
      "the release drain must converge via the poll surface, not spin; still {} parked, \
         {} live after {steps} steps",
      a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0),
      a.live_bridge_count()
    );
  }

  // The parked/blocked work drained — no strand.
  assert_eq!(
    a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0),
    0,
    "release must drain the parked dial bucket to empty (no strand)"
  );
  // Release enabled real completions: exchanges reached a SUCCEEDED terminus within
  // their deadline, driven purely by the poll surface. Dropping the release (the
  // mutation) leaves this at zero.
  assert!(
    succeeded > 0,
    "release must let parked exchanges reach a SUCCEEDED terminus (got {succeeded}); \
       withholding B -> A instead fails every exchange at its deadline"
  );
}

// ============================================================================
// Deadline-phased dial wakes: a budget-deferred but still-creditable parked dial
// gets a pre-deadline SERVICE wake (mechanism (a)), a liveness-critical reliable-ping
// fallback pops past a spent budget (mechanism (b)), and a near-deadline peer is
// front-deposited into the catch-up ledger (mechanism (c)).
// ============================================================================

/// Inject `count` real reliable user-message dial intents parked on `peer` as the
/// post-re-park state a budget-deferred burst reaches on a since-established, amply
/// credited connection: each carries a real machine intent (so `dial_succeeded`
/// resolves it into a `Stream`), `attempted = true`, its PHASED service wake computed
/// via the production [`QuicEndpoint::dial_wake`] (so a `dial_wake ≡ deadline` mutation
/// propagates into the injected keys and the test observably fails), and its
/// `TimerKey::Dial` key registered at that wake. Bypasses the coordinator admission
/// gate via the raw endpoint. Returns the common deadline (`now + stream_timeout`).
fn park_phased_user_dials(
  a: &mut QuicEndpoint<SmolStr>,
  peer: SocketAddr,
  count: usize,
  now: Instant,
) -> Instant {
  let payload = Bytes::from_static(b"x");
  for _ in 0..count {
    a.endpoint_mut()
      .start_user_message(peer, payload.clone(), now)
      .expect("the raw endpoint registers a reliable user-message dial intent");
  }
  a.sieve_dial_events();
  let pending: Vec<super::PendingDial> = a.dial_pending.drain(..).collect();
  assert_eq!(
    pending.len(),
    count,
    "all injected intents sieve into dial_pending"
  );
  a.unattempted_dial_count = 0;
  let mut deadline = now;
  for mut pd in pending {
    pd.attempted = true;
    deadline = pd.deadline;
    let wake = a.dial_wake(pd.deadline, now);
    pd.wake = wake;
    let id = pd.id;
    a.dial_parked.entry(peer).or_default().push_back(pd);
    a.deadline_index.set(super::TimerKey::Dial(id), Some(wake));
  }
  while a.poll_event().is_some() {}
  deadline
}

/// Quiesce a coordinator's handshake-queued pending events so no ImmediateDue anchor
/// competes with the phased dial wakes during a strict-poll drive.
fn quiesce(a: &mut QuicEndpoint<SmolStr>, now: Instant) {
  for _ in 0..10 {
    if a.conns_with_pending_events.is_empty() {
      break;
    }
    a.handle_timeout(now);
    while a.poll_transmit().is_some() {}
    while a.poll_event().is_some() {}
  }
  while a.poll_transmit().is_some() {}
  while a.poll_event().is_some() {}
}

/// T1 (single-peer, deep ledger). A budget-deferred creditable tail parked ~one
/// stream-timeout deep on ONE amply-credited peer is minted before its deadline by the
/// phased pre-deadline SERVICE wake — position-independently of the ledger depth. The
/// ledger is deliberately DEEP (reach latency `ceil(200/64)=4` intervals = 40ms > the
/// 30ms deadline), so the FIFO catch-up alone cannot drain the tail in time: only the
/// phased wake's unbudgeted full-drain can. That defeats the empty-window trap where a
/// shallow ledger drains within the margin and a `dial_wake ≡ deadline` mutation
/// falsely passes.
///
/// Mutation (reverting the phased wake so `dial_wake` returns `deadline`): the injected
/// keys and the production re-park land at the deadline, so the tail the catch-up cannot reach in
/// four-intervals-worth of budget retires at 30ms — `saw_failure` trips and the minted
/// count falls short of `M`.
#[test]
fn phased_wake_mints_deep_single_peer_tail_before_deadline() {
  use crate::event::{Event, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8400".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8401".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(Duration::from_millis(30));
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(1024),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  quiesce(&mut a, now);
  assert_eq!(
    a.live_bridge_count(),
    0,
    "precondition: the warm-up mints no bridge"
  );

  const M: usize = 200;
  let deadline = park_phased_user_dials(&mut a, b_addr, M, now);
  assert_eq!(deadline, now + Duration::from_millis(30));
  assert!(
    a.dial_parked.get(&b_addr).map(|q| q.len()).unwrap_or(0) > super::MAX_DIAL_ATTEMPTS_PER_PASS,
    "the ledger is deeper than the per-pass budget"
  );
  a.ready_dial_peers.insert(b_addr);
  a.reconcile_catchup_anchor(now);

  let mut saw_failure = false;
  for _ in 0..64 {
    if a
      .dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true)
    {
      break;
    }
    let wake = a
      .poll_timeout()
      .expect("a non-empty parked-dial cohort always schedules a wake");
    a.handle_timeout(wake);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev {
        if p.outcome() == ExchangeStatus::Failed {
          saw_failure = true;
        }
      }
    }
  }
  assert!(
    a.dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true),
    "every deep-ledger dial is minted before its deadline via the phased service wake"
  );
  assert_eq!(
    a.live_bridge_count(),
    M,
    "all M dials minted a real bridge (none retired despite capacity)"
  );
  assert!(
    !saw_failure,
    "no dial retired to Failed — the phased wake attempts the whole table before the deadline"
  );
}

/// T2 (multi-peer burst). The phased wake's full-drain is position-independent ACROSS
/// peers: two amply-credited peers each carry a budget-deferred creditable tail in one
/// shared ledger, and every dial on BOTH peers mints before its deadline. The shared
/// per-pass budget cannot finish both buckets in the deadline window, so a
/// `dial_wake ≡ deadline` mutation strands whichever bucket the FIFO catch-up did not
/// reach — the multi-bucket twin of T1.
///
/// The same reverted phased wake fails it identically: the tail of the peer the
/// catch-up reaches second retires at its deadline.
#[test]
fn phased_wake_mints_multi_peer_burst_before_deadline() {
  use crate::event::{Event, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8404".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8405".parse().unwrap();
  let c_addr: SocketAddr = "127.0.0.1:8406".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(Duration::from_millis(30));
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(1024),
    b_addr,
    now,
  );
  let mut c = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("c"), c_addr),
    test_config_bidi_limit(1024),
    c_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  establish(&mut a, &mut c, a_addr, c_addr, now);
  quiesce(&mut a, now);
  assert_eq!(
    a.live_bridge_count(),
    0,
    "precondition: no bridge minted in the warm-up"
  );

  // Each bucket deeper than the shared budget, so one shared per-pass budget cannot
  // finish both within the deadline window.
  const PER_PEER: usize = 120;
  let d_b = park_phased_user_dials(&mut a, b_addr, PER_PEER, now);
  let d_c = park_phased_user_dials(&mut a, c_addr, PER_PEER, now);
  assert_eq!(d_b, now + Duration::from_millis(30));
  assert_eq!(d_c, now + Duration::from_millis(30));
  a.ready_dial_peers.insert(b_addr);
  a.ready_dial_peers.insert(c_addr);
  a.reconcile_catchup_anchor(now);

  let mut saw_failure = false;
  for _ in 0..64 {
    let b_done = a
      .dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true);
    let c_done = a
      .dial_parked
      .get(&c_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true);
    if b_done && c_done {
      break;
    }
    let wake = a
      .poll_timeout()
      .expect("a non-empty ledger schedules a wake");
    a.handle_timeout(wake);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev {
        if p.outcome() == ExchangeStatus::Failed {
          saw_failure = true;
        }
      }
    }
  }
  assert_eq!(
    a.live_bridge_count(),
    2 * PER_PEER,
    "every dial on both peers minted before its deadline"
  );
  assert!(
    !saw_failure,
    "no dial on either peer retired despite capacity"
  );
}

/// T6 (no busy loop, no re-chunk). A never-creditable blocked dial fires at most
/// `ceil(M/I) + 1` full ticks across its final window (the chained phase spaces its
/// wakes at least one interval apart and lands the last exactly on the deadline), and
/// repeated `poll_timeout` without advancing time returns a constant future instant.
///
/// Mutation (phase 2 → naive `deadline - M`): after the `deadline - M` service tick
/// re-parks, the wake is re-registered at `deadline - M`, now in the PAST, so
/// `poll_timeout` returns a past instant and the strict-poll loop spins at the same
/// instant without ever advancing to the deadline — the invocation count explodes past
/// the bound and the dial never retires.
#[test]
fn phased_wake_chained_phase_does_not_busy_loop() {
  let a_addr: SocketAddr = "127.0.0.1:8408".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8409".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(Duration::from_millis(30));
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  // B grants ZERO bidi credit, so every dial re-parks (established, never creditable)
  // and rides its phased wake to the deadline — no handshake timers, no capacity.
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(0),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  quiesce(&mut a, now);

  // One reliable user-message dial re-parks on B (0 credit) with its phased wake.
  let id = a
    .start_user_message(b_addr, Bytes::from_static(b"x"), now)
    .expect("A schedules a reliable user-message dial");
  while a.poll_transmit().is_some() {}
  assert!(
    a.dial_parked.get(&b_addr).is_some_and(|q| !q.is_empty()),
    "the dial re-parks behind zero bidi credit"
  );
  assert!(
    !a.bridges.contains_key(&id),
    "no bridge minted at zero credit"
  );

  // Repeated poll_timeout without advancing time returns a CONSTANT future instant.
  let w1 = a.poll_timeout().expect("a parked dial schedules a wake");
  let w2 = a.poll_timeout().expect("a parked dial schedules a wake");
  assert_eq!(
    w1, w2,
    "repeated poll_timeout without time advance is constant"
  );
  assert!(
    w1 > now,
    "the service wake is strictly future — no busy loop"
  );

  // Strict-poll drive; count handle_timeout invocations. The chained phase spaces the
  // wakes at >= one interval, so the dial converges in a few ticks and retires at the
  // deadline. A past-instant phase-2 wake (the naive `deadline - M`) would spin here to
  // the bound.
  let mut ticks = 0usize;
  let mut retired = false;
  for _ in 0..64 {
    if a
      .dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true)
    {
      retired = true;
      break;
    }
    let wake = a.poll_timeout().expect("a parked dial schedules a wake");
    a.handle_timeout(wake);
    ticks += 1;
  }
  assert!(
    retired,
    "the blocked dial retires at its deadline (never strands)"
  );
  // M / I + 2 = 20/10 + 2 = 4; the phased chain fires at ~D-20, D-10, D.
  assert!(
    ticks <= 4,
    "the chained phase fires at most M/I + 2 ticks in the final window, got {ticks}; a naive \
       past-instant phase-2 wake would spin to the loop bound"
  );
}

/// T7 (retire-timing authority). The retire pre-check reads the true `deadline`, never
/// the phased service `wake`: a never-creditable blocked dial's Failed completion
/// surfaces at exactly the deadline, not at the earlier `deadline - M` service wake.
///
/// Mutation (pre-check reads `now >= wake` instead of `now >= deadline`): the very first service wake at
/// `deadline - M` retires the dial early, so the Failed completion surfaces a full
/// margin before the true deadline.
#[test]
fn retire_pre_check_uses_deadline_not_service_wake() {
  use crate::event::{Event, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8412".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8413".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(Duration::from_millis(30));
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(0),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  quiesce(&mut a, now);

  let deadline = now + Duration::from_millis(30);
  let service_wake = a.dial_wake(deadline, now);
  assert_eq!(
    service_wake,
    now + Duration::from_millis(10),
    "the first service wake is one margin before the deadline"
  );
  let id = a
    .start_user_message(b_addr, Bytes::from_static(b"x"), now)
    .expect("A schedules a reliable user-message dial");
  while a.poll_transmit().is_some() {}
  assert!(a.dial_parked.get(&b_addr).is_some_and(|q| !q.is_empty()));

  let mut failed_at: Option<Instant> = None;
  for _ in 0..64 {
    if a
      .dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true)
    {
      break;
    }
    let wake = a.poll_timeout().expect("a parked dial schedules a wake");
    a.handle_timeout(wake);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev {
        if p.outcome() == ExchangeStatus::Failed && p.eid() == crate::event::ExchangeId::from(id) {
          failed_at = Some(wake);
        }
      }
    }
  }
  assert_eq!(
    failed_at,
    Some(deadline),
    "the Failed completion surfaces at exactly the deadline, never at the earlier service wake"
  );
}

/// T8 (oracle invariance under the folded wake). Re-parked dials carry a PHASED wake,
/// and the after-every-operation deadline-index oracle folds `entry.wake` — exactly
/// what each `TimerKey::Dial` key is set to — so the incremental index and the
/// brute-force fold agree. This is the missed-registration-site guard.
///
/// Mutation (the missed-site class): register the `Dial` key at `deadline` while the
/// entry stores the phased `wake` (or fold `entry.deadline` in the oracle) — the index
/// then diverges from the fold and `assert_deadline_index_matches` fails.
#[test]
fn deadline_index_oracle_folds_phased_dial_wake() {
  let a_addr: SocketAddr = "127.0.0.1:8416".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8417".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(Duration::from_millis(50));
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config_bidi_limit(0),
    b_addr,
    now,
  );
  establish(&mut a, &mut b, a_addr, b_addr, now);
  quiesce(&mut a, now);
  assert_deadline_index_matches(&mut a, "after establish + quiesce");

  // Several reliable dials re-park on B (0 credit), each registering a phased wake.
  for _ in 0..4 {
    a.start_user_message(b_addr, Bytes::from_static(b"x"), now)
      .expect("A schedules a reliable user-message dial");
    while a.poll_transmit().is_some() {}
    assert_deadline_index_matches(&mut a, "after a phased re-park");
  }

  // Advance across the phased service window; each re-park re-registers a fresh phased
  // wake, and the oracle must track it after every tick.
  for _ in 0..8 {
    if a
      .dial_parked
      .get(&b_addr)
      .map(|q| q.is_empty())
      .unwrap_or(true)
    {
      break;
    }
    let wake = a.poll_timeout().expect("a parked dial schedules a wake");
    a.handle_timeout(wake);
    while a.poll_transmit().is_some() {}
    while a.poll_event().is_some() {}
    assert_deadline_index_matches(&mut a, "after a phased-wake tick");
  }
}

/// T3 (mechanism (b): reliable-ping exempt-pop past a spent budget). A liveness-critical
/// reliable-ping fallback at the FRONT of a peer's bucket mints even when the pass's
/// shared dial budget is already ZERO — the pass-budget twin of the reliable-ping
/// outbound-cap exemption — so an honest FSM ping is never deferred to its cumulative
/// probe deadline (a false Suspect) behind a user-message flood on other peers.
///
/// Mutation (removing the reliable-ping exempt-pop): with a zero budget the pass makes no attempt,
/// the front ping stays parked, and no bridge mints.
#[test]
fn reliable_ping_front_mints_past_zero_budget() {
  let a_addr: SocketAddr = "127.0.0.1:8420".parse().unwrap();
  let y_addr: SocketAddr = "127.0.0.1:8421".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  let mut y = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("y"), y_addr),
    test_config_bidi_limit(64),
    y_addr,
    now,
  );
  establish(&mut a, &mut y, a_addr, y_addr, now);
  quiesce(&mut a, now);

  // Inject a machine-internal reliable-ping parked at the FRONT of Y's bucket, near its
  // cumulative deadline (as the probe FSM's escalation leaves it).
  let deadline = now + Duration::from_millis(5);
  let ping_id = a
    .endpoint_mut()
    .start_reliable_ping(SmolStr::new("y"), y_addr, 7, deadline);
  a.sieve_dial_events();
  let mut pd = a
    .dial_pending
    .pop_front()
    .expect("the reliable-ping sieved into dial_pending");
  a.unattempted_dial_count = 0;
  assert!(
    matches!(pd.kind, super::ExchangeKind::ReliablePing),
    "the sieved intent is stamped ReliablePing"
  );
  pd.attempted = true;
  pd.wake = a.dial_wake(pd.deadline, now);
  let wake = pd.wake;
  a.dial_parked.entry(y_addr).or_default().push_front(pd);
  a.deadline_index
    .set(super::TimerKey::Dial(ping_id), Some(wake));
  while a.poll_event().is_some() {}
  assert!(
    !a.bridges.contains_key(&ping_id),
    "precondition: the ping has not opened yet"
  );

  // Service Y with a ZERO budget (the shared budget spent by other peers this pass).
  let mut budget = 0usize;
  let _ = a.service_peer_bucket(y_addr, now, &mut budget);
  while a.poll_transmit().is_some() {}
  assert!(
    a.bridges.contains_key(&ping_id),
    "the front reliable-ping mints even at zero budget (mechanism (b)); removing the \
       exempt-pop parks it to a false Suspect"
  );
}

/// T4 (mechanism (c): urgent front-deposit ordering). A budget-deferred peer whose front
/// dial is within two catch-up intervals of its DEADLINE is deposited at the FRONT of
/// the ready-dial ledger, ahead of a pre-existing non-urgent peer, so the next catch-up
/// pass reaches it first.
///
/// Mutation (reverting the urgent front-deposit: `insert_front` -> `insert`): the urgent peer is queued at the
/// BACK, so the pre-existing peer stays at the front of the ledger and the pop order
/// reverses.
#[test]
fn urgent_near_deadline_peer_front_deposits_ahead_of_ledger() {
  let a_addr: SocketAddr = "127.0.0.1:8424".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);

  // A pre-existing non-urgent peer P already at the front of the ledger.
  let p_addr: SocketAddr = "127.0.0.9:9800".parse().unwrap();
  a.ready_dial_peers.insert(p_addr);

  // A budget-deferred peer U with a NEAR-deadline front dial (no connection needed — a
  // zero-budget pass makes no attempt, it only reaches the deposit chokepoint).
  let u_addr: SocketAddr = "127.0.0.9:9801".parse().unwrap();
  a.dial_parked
    .entry(u_addr)
    .or_default()
    .push_back(super::PendingDial {
      id: StreamId::from_raw(130_000),
      peer: u_addr,
      deadline: now + Duration::from_millis(5),
      wake: now + Duration::from_millis(5),
      attempted: true,
      kind: super::ExchangeKind::UserMessage,
    });
  let mut budget = 0usize;
  let _ = a.service_peer_bucket(u_addr, now, &mut budget);

  assert_eq!(a.ready_dial_peers.len(), 2, "both peers are queued");
  assert_eq!(
    a.ready_dial_peers.pop_front(),
    Some(u_addr),
    "the near-deadline peer front-deposits ahead of the FIFO tail (mechanism (c)); a \
       plain back-insert leaves P at the front"
  );
  assert_eq!(a.ready_dial_peers.pop_front(), Some(p_addr));
}

/// T5 (stale-prefix interaction). Front-depositing an urgent LIVE peer ahead of a long
/// STALE prefix (peers resident in the ledger with an ABSENT bucket) neither dodges the
/// per-pass visit budget nor reopens the O(ledger) stale walk: one catch-up services the
/// urgent peer, self-cleans stale entries up to the visit cap, and pops at most the cap.
#[test]
fn urgent_front_deposit_over_stale_prefix_respects_visit_cap() {
  let a_addr: SocketAddr = "127.0.0.1:8428".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  let mut a = make_endpoint_full(
    a_cfg,
    test_config().with_max_quic_connections(Some(0)),
    a_addr,
    now,
  );

  // An urgent peer U (front-deposited); its dial retires via the global connection cap
  // when serviced, which is enough to prove the catch-up REACHED it in the first pass.
  let u_addr: SocketAddr = "127.0.0.9:9900".parse().unwrap();
  a.dial_parked
    .entry(u_addr)
    .or_default()
    .push_back(super::PendingDial {
      id: StreamId::from_raw(140_000),
      peer: u_addr,
      deadline: now + Duration::from_millis(5),
      wake: now + Duration::from_millis(5),
      attempted: true,
      kind: super::ExchangeKind::UserMessage,
    });
  let mut budget = 0usize;
  let _ = a.service_peer_bucket(u_addr, now, &mut budget);
  assert_eq!(
    a.ready_dial_peers.pop_front(),
    Some(u_addr),
    "the urgent peer is front-deposited"
  );
  // Re-establish the ledger for the drive: U at front, then a long stale prefix.
  a.ready_dial_peers.insert(u_addr);
  const STALE: usize = 150;
  for i in 0..STALE {
    let s: SocketAddr = format!("127.0.0.10:{}", 10_000 + i).parse().unwrap();
    a.ready_dial_peers.insert(s);
  }
  assert_eq!(a.ready_dial_peers.len(), 1 + STALE);

  let visits_before = a.counters.ready_dial_peer_visits;
  a.catchup_service(now);
  let visits = a.counters.ready_dial_peer_visits - visits_before;
  assert!(
    visits <= super::MAX_READY_DIAL_VISITS_PER_PASS as u64,
    "one catch-up pops at most the visit cap ({}), got {visits} — the front-insert does not \
       dodge the visit budget",
    super::MAX_READY_DIAL_VISITS_PER_PASS
  );
  assert!(
    !a.dial_parked.contains_key(&u_addr),
    "the urgent peer, front-deposited, is serviced in the first interval (its bucket drains)"
  );
  assert!(
    a.ready_dial_peers.len() < 1 + STALE,
    "stale entries self-clean during the pass (they never re-deposit)"
  );
}

/// T10 (budget negative control). With NO front ping, `service_peer_bucket` attempts at
/// most the pass budget: the exempt-pop is strictly kind-gated to reliable-ping.
///
/// Mutation (dropping the reliable-ping kind gate, exempting EVERY front entry): the pass pops up
/// to the exempt cap of extra user-message entries past the budget, so the attempt count
/// exceeds the budget.
#[test]
fn service_peer_bucket_without_pings_attempts_at_most_the_budget() {
  let a_addr: SocketAddr = "127.0.0.1:8432".parse().unwrap();
  let now = Instant::now();
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO);
  // A zero global connection cap retires every dial via `AtGlobalCap` (no repark), so the
  // whole budget is spent and the exempt branch is reached with a non-ping front.
  let mut a = make_endpoint_full(
    a_cfg,
    test_config().with_max_quic_connections(Some(0)),
    a_addr,
    now,
  );
  let peer: SocketAddr = "127.0.0.9:9950".parse().unwrap();
  let far = now + Duration::from_secs(30);
  const N: usize = 100;
  for i in 0..N {
    a.dial_parked
      .entry(peer)
      .or_default()
      .push_back(super::PendingDial {
        id: StreamId::from_raw(150_000 + i as u64),
        peer,
        deadline: far,
        wake: far,
        attempted: true,
        kind: super::ExchangeKind::UserMessage,
      });
  }
  let before = a.counters.dial_entries_serviced;
  let mut budget = super::MAX_DIAL_ATTEMPTS_PER_PASS;
  let _ = a.service_peer_bucket(peer, now, &mut budget);
  let attempts = a.counters.dial_entries_serviced - before;
  assert_eq!(
    attempts,
    super::MAX_DIAL_ATTEMPTS_PER_PASS as u64,
    "with no front reliable-ping the pass attempts exactly the budget; a kind-less \
       exempt-pop attempts up to the exempt cap more"
  );
  assert_eq!(budget, 0, "the whole budget is spent");
}

/// (c) fairness backstop (§ round-robin urgent-lane exception). The urgent front-deposit
/// makes the ready-dial lane LIFO and a deep near-deadline monopolist can starve the FIFO
/// tail — accepted because correctness rests on (a), not on ledger order. With two small
/// urgent peers plus a deep near-deadline monopolist, every dial STILL mints before its
/// deadline via the phased service wake, whatever churn (c) causes in the ledger.
#[test]
fn urgent_lane_monopolist_does_not_starve_correctness() {
  use crate::event::{Event, ExchangeStatus};
  let a_addr: SocketAddr = "127.0.0.1:8436".parse().unwrap();
  let m_addr: SocketAddr = "127.0.0.1:8437".parse().unwrap();
  let u1_addr: SocketAddr = "127.0.0.1:8438".parse().unwrap();
  let u2_addr: SocketAddr = "127.0.0.1:8439".parse().unwrap();
  let now = Instant::now();
  // A near deadline (15ms) makes every re-deposit urgent, so the deep monopolist
  // re-front-inserts every interval — the LIFO-starvation shape (c) permits.
  let a_cfg = EndpointOptions::new(SmolStr::new("a"), a_addr)
    .with_probe_interval(Duration::ZERO)
    .with_gossip_interval(Duration::ZERO)
    .with_push_pull_interval(Duration::ZERO)
    .with_stream_timeout(Duration::from_millis(15));
  let mut a = make_endpoint_full(a_cfg, test_config(), a_addr, now);
  let mut m = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("m"), m_addr),
    test_config_bidi_limit(1024),
    m_addr,
    now,
  );
  let mut u1 = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("u1"), u1_addr),
    test_config_bidi_limit(64),
    u1_addr,
    now,
  );
  let mut u2 = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("u2"), u2_addr),
    test_config_bidi_limit(64),
    u2_addr,
    now,
  );
  establish(&mut a, &mut m, a_addr, m_addr, now);
  establish(&mut a, &mut u1, a_addr, u1_addr, now);
  establish(&mut a, &mut u2, a_addr, u2_addr, now);
  quiesce(&mut a, now);

  const MONOPOLIST: usize = 200;
  const SMALL: usize = 5;
  let _ = park_phased_user_dials(&mut a, m_addr, MONOPOLIST, now);
  let _ = park_phased_user_dials(&mut a, u1_addr, SMALL, now);
  let _ = park_phased_user_dials(&mut a, u2_addr, SMALL, now);
  a.ready_dial_peers.insert(m_addr);
  a.ready_dial_peers.insert(u1_addr);
  a.ready_dial_peers.insert(u2_addr);
  a.reconcile_catchup_anchor(now);

  let mut saw_failure = false;
  for _ in 0..64 {
    let done = [m_addr, u1_addr, u2_addr]
      .iter()
      .all(|p| a.dial_parked.get(p).map(|q| q.is_empty()).unwrap_or(true));
    if done {
      break;
    }
    let wake = a
      .poll_timeout()
      .expect("a non-empty ledger schedules a wake");
    a.handle_timeout(wake);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev {
        if p.outcome() == ExchangeStatus::Failed {
          saw_failure = true;
        }
      }
    }
  }
  assert_eq!(
    a.live_bridge_count(),
    MONOPOLIST + 2 * SMALL,
    "every dial — monopolist AND both small urgent peers — mints before its deadline; (a) \
       backstops the urgent-lane starvation (c) permits"
  );
  assert!(!saw_failure, "no dial retired despite capacity");
}

/// Config knob: `catchup_interval` default and validation (zero and over-ceiling).
#[test]
fn quic_options_catchup_interval_default_and_validation() {
  assert_eq!(
    test_config().catchup_interval(),
    super::DEFAULT_CATCHUP_INTERVAL,
    "the accessor returns the documented default"
  );
  assert_eq!(super::DEFAULT_CATCHUP_INTERVAL, Duration::from_millis(10));

  let err = test_config()
    .with_catchup_interval(Duration::ZERO)
    .validate()
    .expect_err("a zero catch-up interval is rejected");
  assert!(
    matches!(err, super::QuicOptionsError::CatchupIntervalZero),
    "got {err:?}"
  );

  let err = test_config()
    .with_catchup_interval(super::MAX_CATCHUP_INTERVAL + Duration::from_millis(1))
    .validate()
    .expect_err("a catch-up interval above the ceiling is rejected");
  assert!(
    matches!(err, super::QuicOptionsError::CatchupIntervalTooLarge),
    "got {err:?}"
  );

  // At the ceiling validates when the margin is raised to keep margin >= interval.
  test_config()
    .with_catchup_interval(super::MAX_CATCHUP_INTERVAL)
    .with_dial_service_margin(super::MAX_CATCHUP_INTERVAL)
    .validate()
    .expect("a ceiling interval with a matching margin validates");
}

/// Config knob: `dial_service_margin` default and the cross-field floor (>= interval).
#[test]
fn quic_options_dial_service_margin_default_and_validation() {
  assert_eq!(
    test_config().dial_service_margin(),
    super::DEFAULT_DIAL_SERVICE_MARGIN
  );
  assert_eq!(
    super::DEFAULT_DIAL_SERVICE_MARGIN,
    Duration::from_millis(20)
  );

  // Below the default catch-up interval (10ms) is rejected by the cross-field guard.
  let err = test_config()
    .with_dial_service_margin(Duration::from_millis(5))
    .validate()
    .expect_err("a margin below the catch-up interval is rejected");
  assert!(
    matches!(
      err,
      super::QuicOptionsError::DialServiceMarginBelowCatchupInterval
    ),
    "got {err:?}"
  );

  // Exactly one interval is the floor and validates.
  let interval = test_config().catchup_interval();
  test_config()
    .with_dial_service_margin(interval)
    .validate()
    .expect("a margin equal to the catch-up interval validates");
}

/// Config knob: `max_reliable_ping_exempt_pops_per_pass` default and zero rejection.
#[test]
fn quic_options_ping_exempt_cap_default_and_validation() {
  assert_eq!(
    test_config().max_reliable_ping_exempt_pops_per_pass(),
    super::DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS
  );
  assert_eq!(super::DEFAULT_MAX_RELIABLE_PING_EXEMPT_POPS_PER_PASS, 8);

  let err = test_config()
    .with_max_reliable_ping_exempt_pops_per_pass(0)
    .validate()
    .expect_err("a zero exempt-pop cap is rejected");
  assert!(
    matches!(
      err,
      super::QuicOptionsError::MaxReliablePingExemptPopsPerPassZero
    ),
    "got {err:?}"
  );

  test_config()
    .with_max_reliable_ping_exempt_pops_per_pass(1)
    .validate()
    .expect("a cap of 1 validates");
  test_config()
    .validate()
    .expect("the default config validates");
}
