//! Virtual network: datagram queue and virtual stream pipes.

// pub(crate) items below will be consumed by `cluster.rs`.
// Until then the dead_code lint would fire; suppress it for this module.
#![allow(dead_code)]

use std::{
  collections::{BTreeMap, HashSet, VecDeque},
  net::SocketAddr,
  time::Duration,
};

use rand::{RngExt, SeedableRng, rngs::SmallRng};

use memberlist_proto::{
  Endpoint, EndpointEvent, EndpointOptions, Event, Instant, Stream, StreamCommand, StreamError,
  Transmit,
};
use smol_str::SmolStr;

use crate::{checker::Transition, faults::FaultConfig};

/// A pending datagram in the virtual network.
///
/// Carries the typed `Message<SmolStr, SocketAddr>` directly — no byte-level
/// encode/decode round-trip. The simulation exercises the state machine, not
/// the codec.
#[derive(Debug)]
pub(crate) struct PendingDatagram {
  /// The simulated time at which this datagram may be delivered.
  pub(crate) deliver_at: Instant,
  /// Monotonically increasing enqueue counter used as a tie-breaker when two
  /// datagrams share the same `deliver_at`. Without this, `sort_unstable`
  /// produces a non-deterministic ordering under gossip bursts where many
  /// datagrams arrive at the same simulated instant, making successive runs
  /// with identical inputs produce different membership views.
  pub(crate) seq: u64,
  /// Source address.
  pub(crate) from: SocketAddr,
  /// Destination address.
  pub(crate) to: SocketAddr,
  /// The fully-typed messages this datagram carries, dispatched IN ORDER on
  /// delivery. A plain `Packet` is one message; a `Compound` is its inner
  /// messages. Modeling the bundle (not per-message rows) keeps the
  /// datagram boundary faithful: fault/latency is applied ONCE per
  /// datagram, so a one-shot drop drops the whole compound atomically —
  /// exactly what a real UDP/QUIC compound datagram does (a real driver
  /// `encode_outgoing_compound`s into ONE `send_to`).
  pub(crate) messages: Vec<memberlist_proto::typed::Message<SmolStr, SocketAddr>>,
}

/// A virtual in-progress stream exchange between two endpoints.
///
/// # Transport plane
///
/// A `VirtualStream` models the RELIABLE TCP/QUIC plane — the transport that
/// real memberlist uses for push-pull anti-entropy and reliable user messages.
/// This mirrors memberlist's two-plane split: the unreliable UDP plane
/// (gossip, probes, acks = [`PendingDatagram`]s) carries the full packet-level
/// fault model (probabilistic drop, duplicate, jitter, and the one-shot
/// `drop_next`), while the reliable plane is an ordered, retransmitting
/// connection that masks packet-level loss / duplication / reorder.
///
/// Consequently the datagram packet-faults DELIBERATELY do NOT apply to a
/// `VirtualStream`: applying them would falsely model TCP/QUIC as a lossy
/// datagram channel. A stream fails in exactly the two ways a real reliable
/// connection does:
///
/// * **Connection cut** — either endpoint crashes, or a partition opens across
///   the link. [`Network::step_streams`] reaps the stream BEFORE pumping, so no
///   byte ever crosses a cut link and the exchange cannot complete.
/// * **Latency** — a real exchange incurs RTT. This is not stamped explicitly:
///   a push-pull spans several [`Network::step_streams`] pumps (request → load
///   response → response), and between pumps the simulation clock advances to
///   the next endpoint deadline (gossip / probe timers) and to the latency-
///   delayed dial/ack datagrams. The exchange therefore already consumes
///   virtual time on the order of, and exceeding, the configured 0..80ms
///   latency (empirically ~67ms at zero latency, ~200ms at 80ms latency).
///   That per-exchange RTT is negligible against the 30s push-pull schedule
///   and the multi-second convergence floor, so it is faithfully approximated
///   by the natural step cadence rather than an added per-stream delay.
pub(crate) struct VirtualStream {
  /// Address of the initiating endpoint.
  pub(crate) initiator_addr: SocketAddr,
  /// Stream object on the initiating side.
  pub(crate) initiator: Stream<SmolStr, SocketAddr>,
  /// Address of the accepting endpoint.
  pub(crate) acceptor_addr: SocketAddr,
  /// Stream object on the accepting side.
  pub(crate) acceptor: Stream<SmolStr, SocketAddr>,
}

/// Virtual network owning a set of endpoints and a datagram queue.
pub struct Network {
  /// Endpoints keyed by their advertise address. BTreeMap gives a
  /// deterministic (address-sorted) iteration order across processes, so
  /// `drain_transmits`/`drain_events`/`tick_all`/`process_dial_requests` drive
  /// `seq` assignment and fault-RNG consumption in the same order everywhere.
  pub(crate) endpoints: BTreeMap<SocketAddr, Endpoint<SmolStr, SocketAddr>>,
  /// Addresses of crashed (stopped) endpoints. A crashed node's endpoint
  /// remains in `endpoints` but is skipped by all tick/transmit/deliver
  /// loops until `Cluster::restart` removes it from this set and replaces
  /// the stale endpoint entry with a fresh one.
  pub(crate) crashed: HashSet<SocketAddr>,
  /// Datagrams waiting for `deliver_at <= now`.
  pub(crate) queue: VecDeque<PendingDatagram>,
  /// Fault injection configuration.
  pub(crate) faults: FaultConfig,
  /// Monotonically increasing counter stamped onto each enqueued datagram as
  /// `PendingDatagram::seq`. Used as a `deliver_at` tie-breaker in
  /// `deliver_ready` so delivery order is fully deterministic.
  pub(crate) next_seq: u64,
  /// Seeded RNG used exclusively for probabilistic fault decisions (drop,
  /// duplicate, jitter). Kept separate from per-endpoint RNGs so fault
  /// behavior is independently reproducible.
  pub(crate) fault_rng: SmallRng,
  /// Count of datagrams ACTUALLY probabilistically dropped (the `drop_per_mille`
  /// roll fired). Counts only the probabilistic drop — not crash/partition
  /// drops — so the campaign's non-vacuity assertion proves real fault injection
  /// occurred, not merely that a nonzero rate was configured.
  pub(crate) fault_drops: u64,
  /// Count of datagrams ACTUALLY probabilistically duplicated (the
  /// `duplicate_per_mille` roll fired and the second copy was enqueued).
  pub(crate) fault_duplicates: u64,
}

impl Network {
  /// Create an empty network with no faults.
  pub fn new() -> Self {
    Self {
      endpoints: BTreeMap::new(),
      crashed: HashSet::new(),
      queue: VecDeque::new(),
      faults: FaultConfig::none(),
      next_seq: 0,
      fault_rng: SmallRng::seed_from_u64(0),
      fault_drops: 0,
      fault_duplicates: 0,
    }
  }

  /// Re-seed the fault RNG. Call before the test run to make probabilistic
  /// fault outcomes reproducible.
  pub(crate) fn seed_faults(&mut self, seed: u64) {
    self.fault_rng = SmallRng::seed_from_u64(seed);
  }

  /// Add an endpoint with the given config, returning the advertise address.
  pub fn add_endpoint(
    &mut self,
    cfg: EndpointOptions<SmolStr, SocketAddr>,
    now: Instant,
  ) -> SocketAddr {
    let addr = *cfg.advertise_addr_ref();
    let mut ep = Endpoint::new_at(
      cfg,
      now,
      SmallRng::seed_from_u64(crate::rng_seed_from_addr(&addr)),
    );
    ep.start_scheduling(now);
    self.endpoints.insert(addr, ep);
    addr
  }

  /// Enqueue a single-message datagram for delivery (plain `Packet` / the
  /// stream + cluster callers). Applies fault injection at enqueue time.
  pub(crate) fn enqueue(
    &mut self,
    from: SocketAddr,
    to: SocketAddr,
    message: memberlist_proto::typed::Message<SmolStr, SocketAddr>,
    deliver_at: Instant,
  ) {
    self.enqueue_datagram(from, to, vec![message], deliver_at);
  }

  /// Enqueue ONE datagram carrying `messages` (a plain `Packet` is one; a
  /// `Compound` is its inner messages). Fault injection + latency are
  /// applied ONCE for the whole datagram, so a one-shot drop / partition
  /// drops the entire compound atomically and the inner messages are
  /// delivered together in order — faithfully modeling a real UDP/QUIC
  /// compound datagram (the interim driver `encode_outgoing_compound`s
  /// into a single `send_to`). Per-message enqueue would let
  /// `drop_next_datagram_from` consume on the first inner message and
  /// still deliver the rest — an impossible delivery a real datagram
  /// cannot produce.
  pub(crate) fn enqueue_datagram(
    &mut self,
    from: SocketAddr,
    to: SocketAddr,
    messages: Vec<memberlist_proto::typed::Message<SmolStr, SocketAddr>>,
    deliver_at: Instant,
  ) {
    if messages.is_empty() {
      return;
    }
    // Draw the full fixed fault-roll set FIRST, unconditionally and before ANY
    // gate (crash, partition, one-shot drop, or a NO_* rate mask), so no gate can
    // shift the fault-RNG stream: masking a fault class suppresses its effect
    // without changing the schedule.
    let drop_roll = self.fault_rng.random_range(0..1000u32);
    let jitter_roll: u64 = self.fault_rng.random();
    let dup_roll = self.fault_rng.random_range(0..1000u32);
    let dup_jitter_roll: u64 = self.fault_rng.random();

    // A crashed node neither sends nor receives: drop a datagram with a crashed
    // source or destination. It is lost immediately, never queued, so nothing
    // can be delivered from or to a down process (including after restart).
    if self.crashed.contains(&from) || self.crashed.contains(&to) {
      return;
    }
    if !self.faults.should_deliver(from, to) {
      return;
    }
    if self.faults.drop_per_mille > 0 && drop_roll < self.faults.drop_per_mille {
      self.fault_drops += 1;
      return;
    }
    // One-way (asymmetric) loss on the (from -> to) link, on top of the global
    // rate. `drop_roll` is reused; the directional rate is independent per link.
    if let Some(&rate) = self.faults.directional_drop_per_mille.get(&(from, to)) {
      if rate > 0 && drop_roll < rate {
        self.fault_drops += 1;
        return;
      }
    }
    let scaled = |roll: u64, max: Duration| -> Duration {
      if max.is_zero() {
        Duration::ZERO
      } else {
        Duration::from_nanos(roll % (max.as_nanos() as u64 + 1))
      }
    };
    let base = deliver_at + self.faults.latency;
    let at = base + scaled(jitter_roll, self.faults.jitter);
    let seq = self.next_seq;
    self.next_seq += 1;
    self.queue.push_back(PendingDatagram {
      deliver_at: at,
      seq,
      from,
      to,
      messages: messages.clone(),
    });
    if self.faults.duplicate_per_mille > 0 && dup_roll < self.faults.duplicate_per_mille {
      self.fault_duplicates += 1;
      let at2 = base + scaled(dup_jitter_roll, self.faults.jitter);
      let seq2 = self.next_seq;
      self.next_seq += 1;
      self.queue.push_back(PendingDatagram {
        deliver_at: at2,
        seq: seq2,
        from,
        to,
        messages,
      });
    }
  }

  /// Drain all datagrams whose `deliver_at <= now` and deliver them to the
  /// target endpoint. Returns the number of datagrams delivered.
  pub(crate) fn deliver_ready(&mut self, now: Instant) -> usize {
    self.deliver_ready_inner(now, None)
  }

  /// Like [`deliver_ready`](Self::deliver_ready), but additionally records every
  /// per-message change to the receiving observer's view of each `subject` into
  /// `out` as a [`Transition`], in dispatch order. A compound datagram is
  /// dispatched message-by-message with a row snapshot taken between messages,
  /// so an illegal intermediate value carried by one message and overwritten by
  /// a later message of the SAME datagram (or a second datagram at the same
  /// deadline) is recorded rather than masked. Only the receiving observer's row
  /// can change on a given delivery, so only that row is snapshotted (O(n) per
  /// message, not the full O(n²) matrix). Returns the datagrams delivered.
  pub(crate) fn deliver_ready_recording(
    &mut self,
    now: Instant,
    subjects: &[SmolStr],
    out: &mut Vec<Transition>,
  ) -> usize {
    self.deliver_ready_inner(now, Some((subjects, out)))
  }

  /// Snapshot `observer`'s `(state, incarnation)` view of each `subject`, in
  /// `subjects` order. Returns all-`None` rows for an unknown observer. Used by
  /// the per-message transition recorder; iterating `subjects` in a fixed order
  /// keeps the recorded transition order deterministic.
  fn observer_row(
    &self,
    observer: SocketAddr,
    subjects: &[SmolStr],
  ) -> Vec<(Option<memberlist_proto::typed::State>, Option<u32>)> {
    match self.endpoints.get(&observer) {
      Some(ep) => subjects
        .iter()
        .map(|s| (ep.member_liveness(s), ep.node_incarnation(s)))
        .collect(),
      None => vec![(None, None); subjects.len()],
    }
  }

  /// Shared delivery skeleton for [`deliver_ready`](Self::deliver_ready) and
  /// [`deliver_ready_recording`](Self::deliver_ready_recording). With
  /// `recorder == None` it behaves exactly as the plain drain; with `Some` it
  /// snapshots the receiving observer's row before and after each dispatched
  /// message and pushes a [`Transition`] for every subject whose value changed.
  fn deliver_ready_inner(
    &mut self,
    now: Instant,
    mut recorder: Option<(&[SmolStr], &mut Vec<Transition>)>,
  ) -> usize {
    let mut delivered = 0;
    // Sort by (deliver_at, seq) so delivery order is fully deterministic.
    // `deliver_at` alone is not a stable key — gossip bursts produce many
    // datagrams at the same simulated instant and `sort_unstable` would
    // produce a different permutation on each run. The monotonic `seq`
    // breaks ties in enqueue order, making the sort a total order.
    self
      .queue
      .make_contiguous()
      .sort_unstable_by_key(|d| (d.deliver_at, d.seq));

    let mut remaining = VecDeque::new();
    while let Some(d) = self.queue.pop_front() {
      if d.deliver_at > now {
        remaining.push_back(d);
        break;
      }
      // Drop on the cut-at-delivery condition. A datagram enqueued before a
      // partition was opened (and held back by latency/jitter) must NOT cross a
      // link that is cut at the moment it matures: re-check the partition with
      // the non-consuming `partitioned` predicate, and drop if the receiver is
      // down. Asymmetric with the source: a datagram already on the wire
      // survives the SENDER crashing afterwards (the enqueue-time `from` check
      // already blocked a crashed sender from emitting), so `from` is NOT
      // re-checked here — only the link (`partitioned`) and the receiver
      // (`crashed.contains(&d.to)`).
      if self.faults.partitioned(d.from, d.to) || self.crashed.contains(&d.to) {
        continue;
      }
      if self.dispatch_datagram(d, now, &mut recorder) {
        delivered += 1;
      }
    }
    // Drain rest that are not yet ready.
    while let Some(d) = self.queue.pop_front() {
      if d.deliver_at <= now {
        // Same cut-at-delivery drop as the primary loop: a matured datagram
        // must not cross a freshly-cut link or reach a downed receiver. The
        // sender crashing after enqueue does not retract an in-flight datagram.
        if self.faults.partitioned(d.from, d.to) || self.crashed.contains(&d.to) {
          continue;
        }
        if self.dispatch_datagram(d, now, &mut recorder) {
          delivered += 1;
        }
      } else {
        remaining.push_back(d);
      }
    }
    self.queue = remaining;
    delivered
  }

  /// Dispatch one matured datagram's messages, in order, to its destination
  /// endpoint. Returns `true` if the destination existed (one delivery unit).
  /// When `recorder` is `Some`, the receiving observer's row is snapshotted
  /// before and after each message and a [`Transition`] is pushed for every
  /// subject whose `(state, incarnation)` changed.
  fn dispatch_datagram(
    &mut self,
    d: PendingDatagram,
    now: Instant,
    recorder: &mut Option<(&[SmolStr], &mut Vec<Transition>)>,
  ) -> bool {
    if !self.endpoints.contains_key(&d.to) {
      return false;
    }
    // One datagram ⇒ one delivery unit: dispatch every carried message in
    // order, count the datagram once.
    for m in d.messages {
      match recorder {
        Some((subjects, out)) => {
          let before = self.observer_row(d.to, subjects);
          if let Some(ep) = self.endpoints.get_mut(&d.to) {
            dispatch_message(ep, d.from, m, now);
          }
          let after = self.observer_row(d.to, subjects);
          for (i, subject) in subjects.iter().enumerate() {
            if before[i] != after[i] {
              let (state, incarnation) = after[i];
              // A datagram never prunes: prunes are recorded only around the
              // `tick_all`/`reset_nodes` phase, so `pruned` is always false here.
              out.push(Transition::new(
                d.to,
                subject.clone(),
                state,
                incarnation,
                false,
              ));
            }
          }
        }
        None => {
          if let Some(ep) = self.endpoints.get_mut(&d.to) {
            dispatch_message(ep, d.from, m, now);
          }
        }
      }
    }
    true
  }

  /// Earliest delivery deadline among all queued datagrams.
  ///
  /// Datagrams addressed to a crashed node contribute no deadline — they are
  /// dropped on delivery, so they must not pull the clock forward.
  pub(crate) fn earliest_datagram_deadline(&self) -> Option<Instant> {
    self
      .queue
      .iter()
      .filter(|d| !self.crashed.contains(&d.to))
      .map(|d| d.deliver_at)
      .min()
  }

  /// Earliest `poll_timeout` across all endpoints.
  ///
  /// Crashed endpoints contribute no deadline — they are not being ticked.
  pub(crate) fn earliest_endpoint_deadline(&self) -> Option<Instant> {
    self
      .endpoints
      .iter()
      .filter(|(addr, _)| !self.crashed.contains(addr))
      .filter_map(|(_, ep)| ep.poll_timeout())
      .min()
  }

  /// The next instant the simulation must wake at.
  pub(crate) fn next_deadline(&self) -> Option<Instant> {
    [
      self.earliest_datagram_deadline(),
      self.earliest_endpoint_deadline(),
    ]
    .into_iter()
    .flatten()
    .min()
  }

  /// Drain `poll_transmit` from every endpoint and enqueue the resulting
  /// datagrams in the virtual network. Call this at the start of each
  /// simulation step.
  pub(crate) fn drain_transmits(&mut self, now: Instant) {
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    for addr in addrs {
      if self.crashed.contains(&addr) {
        continue;
      }
      // Collect all pending transmits before calling enqueue to avoid
      // holding a mutable borrow on self.endpoints while mutating self.queue.
      let mut pending: Vec<(
        SocketAddr,
        Vec<memberlist_proto::typed::Message<SmolStr, SocketAddr>>,
      )> = Vec::new();
      let ep = self.endpoints.get_mut(&addr).unwrap();
      while let Some(tx) = ep.poll_transmit() {
        match tx {
          // A plain Packet is one single-message datagram.
          Transmit::Packet(p) => {
            let (to, message) = p.into_parts();
            pending.push((to, vec![message]));
          }
          // A compound is still ONE datagram — its inner messages are
          // delivered (or dropped) together, in order, as a real driver's
          // single encode_outgoing_compound `send_to` would. Per-message
          // enqueue would mis-model the datagram boundary for fault
          // injection.
          Transmit::Compound(cmp) => {
            let (to, messages) = cmp.into_parts();
            pending.push((to, messages));
          }
        }
      }
      for (to, messages) in pending {
        self.enqueue_datagram(addr, to, messages, now);
      }
    }
  }

  /// Drain `poll_event` from every endpoint so progress is observable.
  /// Admission (Alive / join merge) is applied inline by each host's
  /// installed `AliveDelegate` / `MergeDelegate`; this method only needs to
  /// re-enqueue non-`DialRequested` events so `Cluster::poll_event` callers
  /// can still observe `NodeJoined` / `NodeLeft` / … . Returns `true` if any
  /// event fired.
  pub(crate) fn drain_events(&mut self, now: Instant) -> bool {
    // `now` is reserved for future event handlers that need timestamps;
    // today's drain path is time-agnostic.
    let _ = now;
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    let mut any = false;
    for addr in addrs {
      if self.crashed.contains(&addr) {
        continue;
      }
      let ep = self.endpoints.get_mut(&addr).unwrap();
      // Re-enqueue every event (including DialRequested, which
      // `process_dial_requests` consumes separately) so Cluster::poll_event
      // callers can still observe NodeJoined / NodeLeft / … .
      let mut deferred = Vec::new();
      while let Some(ev) = ep.poll_event() {
        any = true;
        deferred.push(ev);
      }
      for ev in deferred {
        ep.requeue_event(ev);
      }
    }
    any
  }

  /// Fire `handle_timeout(now)` on every non-crashed endpoint.
  pub(crate) fn tick_all(&mut self, now: Instant) {
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    for addr in addrs {
      if self.crashed.contains(&addr) {
        continue;
      }
      if let Some(ep) = self.endpoints.get_mut(&addr) {
        ep.handle_timeout(now);
      }
    }
  }

  /// Process all `Event::DialRequested` events from every endpoint.
  ///
  /// For each dial request:
  /// - If fault-injected or the peer is unknown, calls `dial_failed`.
  /// - Otherwise, calls `dial_succeeded` on the initiator and `accept_stream`
  ///   on the target to create a `VirtualStream` pair.
  ///
  /// Returns `true` if any new streams were established.
  pub(crate) fn process_dial_requests(
    &mut self,
    streams: &mut Vec<VirtualStream>,
    now: Instant,
  ) -> bool {
    let addrs: Vec<SocketAddr> = self.endpoints.keys().copied().collect();
    let mut any = false;
    for addr in addrs {
      if self.crashed.contains(&addr) {
        continue;
      }
      // Collect DialRequested before mutably borrowing individual endpoints;
      // re-enqueue every other event so Cluster::poll_event callers can
      // observe NodeJoined / NodeUpdated / NodeConflict etc.
      let pending: Vec<_> = {
        let ep = self.endpoints.get_mut(&addr).unwrap();
        let mut v = Vec::new();
        let mut deferred = Vec::new();
        while let Some(ev) = ep.poll_event() {
          match ev {
            Event::DialRequested(dial) => {
              let (id, peer, _) = dial.into_parts();
              v.push((id, peer));
            }
            other => {
              deferred.push(other);
            }
          }
        }
        for ev in deferred {
          ep.requeue_event(ev);
        }
        v
      };
      for (id, peer_addr) in pending {
        // Stream dials are cut only by a partition, not by the one-shot
        // datagram drop. Use the non-consuming `partitioned` predicate so
        // `drop_next` is not consumed here and remains available for the next
        // datagram emitted from `addr`.
        if self.faults.partitioned(addr, peer_addr) {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_failed(id, StreamError::Timeout, now);
          continue;
        }
        // A crashed peer cannot accept new streams.
        if self.crashed.contains(&peer_addr) {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_failed(id, StreamError::Timeout, now);
          continue;
        }
        // Unknown peer → dial failed.
        if !self.endpoints.contains_key(&peer_addr) {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_failed(id, StreamError::Timeout, now);
          continue;
        }
        // Establish the virtual stream pair.
        let initiator_stream = {
          let ep = self.endpoints.get_mut(&addr).unwrap();
          ep.dial_succeeded(id, now)
        };
        let Some(initiator_stream) = initiator_stream else {
          continue;
        };
        let acceptor_stream = {
          let ep = self.endpoints.get_mut(&peer_addr).unwrap();
          ep.accept_stream(addr, now)
        };
        // A leaving/left acceptor admits no new stream; skip establishing the
        // pair if it declined to accept.
        let Some(acceptor_stream) = acceptor_stream else {
          continue;
        };
        streams.push(VirtualStream {
          initiator_addr: addr,
          initiator: initiator_stream,
          acceptor_addr: peer_addr,
          acceptor: acceptor_stream,
        });
        any = true;
      }
    }
    any
  }

  /// Pipe bytes between active virtual streams each tick.
  ///
  /// Streams are the RELIABLE TCP/QUIC plane (see [`VirtualStream`]). The
  /// unreliable-datagram packet faults — probabilistic drop / duplicate /
  /// jitter and the one-shot `drop_next` — are NOT applied here: a reliable
  /// connection masks packet-level loss / duplication / reorder, so modeling it
  /// as lossy would be unfaithful. A stream's only faults are connection cut
  /// (crash or partition, reaped below before any pumping) and latency
  /// (approximated by the natural step cadence, per [`VirtualStream`]).
  ///
  /// For each surviving stream pair:
  /// 1. Drain initiator → acceptor bytes via `poll_transmit` / `handle_data`.
  /// 2. Drain acceptor → initiator bytes via `poll_transmit` / `handle_data`.
  /// 3. Route `EndpointEvent`s from each stream back into its owning endpoint
  ///    and apply any returned `StreamCommand`.
  /// 4. Fire `handle_timeout(now)` on each stream.
  ///
  /// Completed (both sides done) and failed streams are removed from `streams`.
  pub(crate) fn step_streams(&mut self, streams: &mut Vec<VirtualStream>, now: Instant) {
    self.step_streams_inner(streams, now, None);
  }

  /// Like [`step_streams`](Self::step_streams), but additionally records every
  /// per-event change to the endpoint membership a routed `EndpointEvent` causes
  /// into `out` as a [`Transition`], in event-dispatch order. A push-pull
  /// `EndpointEvent` (request or reply) merges remote state inline via
  /// `Endpoint::handle_stream_event`, so two same-step stream events to the SAME
  /// observer can each mutate the same `(observer, subject)` row; recording at
  /// the per-event boundary surfaces an illegal intermediate that a later
  /// same-step event would otherwise overwrite — the reliable-plane sibling of
  /// the per-message datagram recorder. Only the endpoint receiving the event
  /// can change its own row, so just that observer's row is snapshotted before
  /// and after each `handle_stream_event` call (O(n) per event). A stream never
  /// prunes, so the recorded transitions all carry `pruned == false`.
  pub(crate) fn step_streams_recording(
    &mut self,
    streams: &mut Vec<VirtualStream>,
    now: Instant,
    subjects: &[SmolStr],
    out: &mut Vec<Transition>,
  ) {
    self.step_streams_inner(streams, now, Some((subjects, out)));
  }

  /// Shared stream-pumping skeleton for [`step_streams`](Self::step_streams) and
  /// [`step_streams_recording`](Self::step_streams_recording). With
  /// `recorder == None` it behaves exactly as the plain pump; with `Some` it
  /// snapshots the receiving observer's row before and after each routed
  /// `EndpointEvent` and pushes a [`Transition`] for every subject whose
  /// `(state, incarnation)` changed.
  fn step_streams_inner(
    &mut self,
    streams: &mut Vec<VirtualStream>,
    now: Instant,
    mut recorder: Option<(&[SmolStr], &mut Vec<Transition>)>,
  ) {
    // Reap any established stream whose link is now cut — either endpoint
    // crashed, or a partition has been opened between them since the stream
    // was dialed. A real TCP/QUIC connection across a cut link stops carrying
    // bytes, so the exchange must NOT complete: dropping the `VirtualStream`
    // destroys both `Stream` halves, so neither side is pumped again and no
    // push-pull reply (anti-entropy) ever crosses the cut. The endpoint
    // retains no per-stream state for a PushPull once `dial_succeeded`
    // removed its pending intent, so nothing leaks; a `ReliablePing`
    // fallback is bounded by the probe's own cumulative deadline. Done before
    // pumping so this tick cannot deliver a byte across a freshly-cut link.
    streams.retain(|vs| {
      !(self.crashed.contains(&vs.initiator_addr)
        || self.crashed.contains(&vs.acceptor_addr)
        || self.faults.partitioned(vs.initiator_addr, vs.acceptor_addr))
    });

    let mut buf = Vec::with_capacity(4096);
    for vs in streams.iter_mut() {
      // ── Initiator → Acceptor ────────────────────────────────────────────
      buf.clear();
      if vs.initiator.poll_transmit(now, &mut buf).is_some() && !buf.is_empty() {
        // Ignoring Err: a decode failure terminalizes the stream, which the
        // step loop's stream-drain pass reaps; nothing actionable here.
        let _ = vs.acceptor.handle_data(&buf, now);
      }
      // ── Acceptor → Initiator ────────────────────────────────────────────
      buf.clear();
      if vs.acceptor.poll_transmit(now, &mut buf).is_some() && !buf.is_empty() {
        // Ignoring Err: same terminality-as-reap reasoning as the
        // initiator → acceptor direction above.
        let _ = vs.initiator.handle_data(&buf, now);
      }
      // ── Route EndpointEvents (initiator) ────────────────────────────────
      while let Some(ep_ev) = vs.initiator.poll_endpoint_event() {
        Self::route_stream_event(
          &mut self.endpoints,
          vs.initiator_addr,
          &mut vs.initiator,
          ep_ev,
          now,
          &mut recorder,
        );
      }
      // ── Route EndpointEvents (acceptor) ─────────────────────────────────
      while let Some(ep_ev) = vs.acceptor.poll_endpoint_event() {
        Self::route_stream_event(
          &mut self.endpoints,
          vs.acceptor_addr,
          &mut vs.acceptor,
          ep_ev,
          now,
          &mut recorder,
        );
      }
      // ── Timeouts ────────────────────────────────────────────────────────
      vs.initiator.handle_timeout(now);
      vs.acceptor.handle_timeout(now);
    }
    // Remove streams where both sides are done, or either side has failed.
    streams.retain(|vs| {
      if vs.initiator.is_failed().is_some() || vs.acceptor.is_failed().is_some() {
        return false;
      }
      !(vs.initiator.is_done() && vs.acceptor.is_done())
    });
  }

  /// Route one `EndpointEvent` from `stream` (owned by the endpoint at
  /// `observer`) back into that endpoint, applying any returned
  /// `StreamCommand`. When `recorder` is `Some`, the observer's row over all
  /// `subjects` is snapshotted before and after `handle_stream_event` (the only
  /// call that can merge remote membership inline) and a [`Transition`] is
  /// pushed for every subject whose `(state, incarnation)` changed. Taking
  /// `endpoints` by `&mut` (rather than `&mut self`) keeps the borrow off `self`
  /// so the caller can still hold the `&mut recorder` it owns.
  fn route_stream_event(
    endpoints: &mut BTreeMap<SocketAddr, Endpoint<SmolStr, SocketAddr>>,
    observer: SocketAddr,
    stream: &mut Stream<SmolStr, SocketAddr>,
    ep_ev: EndpointEvent<SmolStr, SocketAddr>,
    now: Instant,
    recorder: &mut Option<(&[SmolStr], &mut Vec<Transition>)>,
  ) {
    let row_of = |endpoints: &BTreeMap<SocketAddr, Endpoint<SmolStr, SocketAddr>>,
                  subjects: &[SmolStr]| {
      match endpoints.get(&observer) {
        Some(ep) => subjects
          .iter()
          .map(|s| (ep.member_liveness(s), ep.node_incarnation(s)))
          .collect::<Vec<_>>(),
        None => vec![(None, None); subjects.len()],
      }
    };
    match recorder {
      Some((subjects, out)) => {
        let before = row_of(endpoints, subjects);
        if let Some(ep) = endpoints.get_mut(&observer) {
          if let Some(cmd) = ep.handle_stream_event(ep_ev, now) {
            apply_stream_command(stream, cmd, now);
          }
        }
        let after = row_of(endpoints, subjects);
        for (i, subject) in subjects.iter().enumerate() {
          if before[i] != after[i] {
            let (state, incarnation) = after[i];
            // A stream merge never prunes: prunes are recorded only around the
            // `tick_all`/`reset_nodes` phase, so `pruned` is always false here.
            out.push(Transition::new(
              observer,
              subject.clone(),
              state,
              incarnation,
              false,
            ));
          }
        }
      }
      None => {
        if let Some(ep) = endpoints.get_mut(&observer) {
          if let Some(cmd) = ep.handle_stream_event(ep_ev, now) {
            apply_stream_command(stream, cmd, now);
          }
        }
      }
    }
  }
}

impl Default for Network {
  fn default() -> Self {
    Self::new()
  }
}

/// Apply a `StreamCommand` returned by `Endpoint::handle_stream_event` to the
/// appropriate `Stream`.
///
/// For `SendPushPullResponse`: encodes the response payload and loads it into
/// the stream's output buffer via `stream_load_response`.
fn apply_stream_command(
  stream: &mut Stream<SmolStr, SocketAddr>,
  cmd: StreamCommand<SmolStr, SocketAddr>,
  now: Instant,
) {
  match cmd {
    StreamCommand::SendPushPullResponse(resp) => {
      let (local_states, user_data) = resp.into_parts();
      let encoded =
        Endpoint::<SmolStr, SocketAddr>::encode_push_pull_response(&local_states, user_data, false);
      let deadline = now + Duration::from_secs(5);
      Endpoint::<SmolStr, SocketAddr>::stream_load_response(stream, encoded, deadline);
    }
    StreamCommand::Close => {
      // Stream will transition to Done on its own after sending/receiving.
    }
  }
}

/// Route a typed `Message` to the appropriate `Endpoint` handler.
///
/// Delegates to the endpoint's public packet entry point, which applies the
/// post-leave inertness gate and dispatches each variant. PushPull and
/// ErrorResponse arrive via the stream layer and are dropped here.
fn dispatch_message(
  ep: &mut Endpoint<SmolStr, SocketAddr>,
  from: SocketAddr,
  msg: memberlist_proto::typed::Message<SmolStr, SocketAddr>,
  now: Instant,
) {
  ep.handle_packet(from, msg, now);
}

#[cfg(test)]
mod tests;
