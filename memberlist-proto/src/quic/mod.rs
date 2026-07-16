//! Composed QUIC + memberlist Sans-I/O coordinator.
//!
//! One conceptual UDP socket. Inbound: first-byte demux (>=0x40 -> quinn,
//! 1..=15 -> memberlist codec). Reliable exchanges ride per-peer QUIC bidi
//! streams; unreliable gossip rides plain UDP. The per-tick step order is
//! load-bearing: stream endpoint-events are drained into the Endpoint
//! BEFORE the memberlist probe `handle_timeout` (else a fallback-ping ack
//! that lands the same tick the probe cumulative deadline expires is lost
//! and the peer is wrongly Suspected).

mod bridge;
#[cfg(feature = "tls")]
mod config;
mod conn;
pub mod crypto;
mod deadline;
mod demux;
mod transport_mode;

#[cfg(feature = "tls")]
pub use config::{QuicConfigError, QuicConfigOptions};
pub use crypto::{
  DEFAULT_MAX_PENDING_CONNECTIONS_PER_SOURCE, DEFAULT_MAX_QUIC_CONNECTIONS,
  DEFAULT_MAX_QUIC_INBOUND_STREAMS, QuicOptions,
};
pub use transport_mode::{DatagramSendStatus, UnreliableTransport};

use crate::Instant;
use core::net::SocketAddr;
use rand::{Rng, rngs::SmallRng};
use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use quinn_proto::{
  ConnectionHandle, DatagramEvent, Dir, Endpoint as QuinnEndpoint, StreamId as QuicSid,
};
use rustc_hash::FxBuildHasher;
use smallvec_wrapper::{MediumVec, SmallVec};

use crate::{
  FxHashSet,
  endpoint::Endpoint,
  error::{Error, StreamError},
  event::{
    Event, ExchangeCompleted, ExchangeId, ExchangeKind, ExchangeStatus, PushPullKind, StreamId,
    Transmit,
  },
};
use bridge::Bridge;
use conn::ConnTable;
use deadline::{DeadlineIndex, TimerKey};
use demux::{Class, classify};
use std::collections::VecDeque;
// `HashSet` indexes connections with a deferred `ConnectionEvent` backlog (the
// immediate-due aggregate) and types a `#[cfg(test)]` snapshot parameter.
use std::collections::HashSet;

/// Record machine stream `id` under connection `ch` in the per-connection
/// bridge index [`QuicEndpoint::bridges_by_conn`], and stamp the pushed position
/// as the bridge's [`Bridge::conn_slot`] back-pointer so a later reap can
/// `swap_remove` that slot in O(1). Invariant on return:
/// `bridges_by_conn[ch][bridges[&id].conn_slot()] == id`.
///
/// A free function taking only the two index fields (never all of `self`) so a
/// mint site can call it while a `self.conns.get_mut(..)` connection borrow is
/// still live — the accept loop mints an inbound bridge inside the per-connection
/// `poll()` drain, where `self.conns` is borrowed mutably. `bridges` and
/// `bridges_by_conn` are disjoint from `conns`, so both are borrowable here.
fn index_bridge_mint<I>(
  bridges: &mut HashMap<StreamId, Bridge<I, SocketAddr>>,
  bridges_by_conn: &mut HashMap<ConnectionHandle, SmallVec<StreamId>>,
  ch: ConnectionHandle,
  id: StreamId,
) {
  let bucket = bridges_by_conn.entry(ch).or_default();
  let slot = bucket.len();
  bucket.push(id);
  // The caller inserts the bridge into `bridges` immediately before this mint,
  // so `get_mut` succeeds; guard defensively rather than panic on a future
  // caller that reorders the two.
  if let Some(br) = bridges.get_mut(&id) {
    br.set_conn_slot(slot);
  }
}

/// Push machine stream `id` onto the pass-scoped ready-bridge queue
/// [`QuicEndpoint::ready_bridges`] so the current servicing pass pumps it
/// exactly once, deduplicated in O(1) by the bridge's [`Bridge::queued`] flag
/// (set here on enqueue, cleared at pump entry). A no-op if `id` names no live
/// bridge — a stale readiness event for a refused or already-reaped stream — or
/// if the bridge is already queued; both are O(1).
///
/// A free function taking only the two disjoint index fields (never all of
/// `self`), so a readiness trigger inside the per-connection `poll()` drain can
/// enqueue while a `self.conns.get_mut(..)` connection borrow is still live —
/// `bridges` and `ready_bridges` are disjoint from `conns`. Mirrors the
/// split-borrow shape of [`index_bridge_mint`].
fn enqueue_ready_bridge<I>(
  bridges: &mut HashMap<StreamId, Bridge<I, SocketAddr>>,
  ready_bridges: &mut VecDeque<StreamId>,
  id: StreamId,
) {
  if let Some(br) = bridges.get_mut(&id) {
    if !br.queued() {
      br.set_queued(true);
      ready_bridges.push_back(id);
    }
  }
}

/// Maximum entries buffered in `mem_ingress` from the QUIC datagram receive
/// drain. quinn's `datagram_receive_buffer_size` bounds inbound BYTES but not
/// entry COUNT (a flood of tiny or zero-length DATAGRAM frames adds ~0 bytes
/// yet one coordinator-queue entry each), so the drain enforces an explicit
/// count cap: beyond it datagrams are popped from quinn and dropped (counted),
/// keeping the coordinator queue and per-tick decode work bounded. Sized far
/// above any legitimate buffered-ingress burst, so normal traffic never drops.
const MAX_MEM_INGRESS_DATAGRAMS: usize = 8192;

/// Maximum inbound application datagrams a SINGLE peer may hold as its STANDING
/// share of the shared `mem_ingress` across the WHOLE undrained queue (counted
/// via `mem_ingress_per_peer`, maintained on every push and pop), not merely
/// within one `service_quinn` pass. Bounds one peer's standing share of the
/// node-global queue so a flooding peer cannot starve other peers' probe Acks
/// regardless of how many recv passes a driver batches before decoding; excess
/// is popped from quinn (so its buffer cannot accumulate) and dropped.
const MAX_INGRESS_DATAGRAMS_PER_PEER: usize = 1024;

/// Push one inbound unreliable payload (a QUIC datagram or a plain-UDP gossip
/// frame) into the shared coordinator ingress queue, enforcing the per-peer
/// standing-share cap AND the node-global cap so neither source can exceed the
/// bound. Drops and counts when either cap is reached; returns whether it was
/// queued. The per-peer counter is maintained here so a flood on EITHER
/// transport cannot starve another peer's probe Ack and the global cap is a
/// hard memory bound regardless of source.
///
/// The payload is supplied as a thunk and built ONLY on admission: a rejected
/// frame never materializes its `Bytes`, so a saturated queue cannot force an
/// allocation/copy per dropped frame on the unauthenticated plain-UDP path
/// (where the payload would otherwise be a fresh `Bytes::copy_from_slice`).
///
/// Borrows only the three disjoint ingress fields (never all of `self`) so the
/// QUIC datagram drain in `service_quinn` can call it while the
/// `self.conns.get_mut(..)` connection borrow is still live.
fn push_mem_ingress_capped(
  mem_ingress: &mut VecDeque<(SocketAddr, Bytes)>,
  per_peer: &mut HashMap<SocketAddr, usize>,
  dropped: &mut u64,
  from: SocketAddr,
  make_payload: impl FnOnce() -> Bytes,
) -> bool {
  let queued = per_peer.get(&from).copied().unwrap_or(0);
  if queued >= MAX_INGRESS_DATAGRAMS_PER_PEER || mem_ingress.len() >= MAX_MEM_INGRESS_DATAGRAMS {
    // Reject WITHOUT constructing the payload: a saturated queue must not let a
    // flood force an allocation/copy per dropped frame on the unauthenticated
    // UDP path.
    *dropped = dropped.saturating_add(1);
    return false;
  }
  mem_ingress.push_back((from, make_payload()));
  *per_peer.entry(from).or_insert(0) += 1;
  true
}

/// One pending dial intent the coordinator owes a `service_dials` attempt to.
///
/// `attempted` distinguishes a freshly-sieved entry (never yet processed by
/// `service_dials`) from one that has been processed at least once. Freshly-
/// sieved entries get an immediate-due wake out of `poll_timeout` so a caller
/// that advances solely by `poll_timeout` cannot orphan them: a caller that
/// drains `poll_event` (sieving `DialRequested` into `dial_pending`) and then
/// waits on `poll_timeout` alone would otherwise only see the intent's own
/// `deadline` and wake at `now + stream_timeout`, by which point
/// `service_dials` would discover the deadline elapsed and consume the intent
/// via `dial_failed`. Once `service_dials` attempts the entry (whether it
/// completes, requeues because the connection is still handshaking, or
/// requeues because of `MAX_STREAMS` credit exhaustion), `attempted` becomes
/// `true` and stays `true` across requeues: future wake-ups are driven by the
/// connection's own `poll_timeout` (handshake completion / credit recovery)
/// and the intent's `deadline`. Immediately re-firing an attempted entry
/// would busy-loop a still-handshaking connection.
struct PendingDial {
  id: StreamId,
  peer: SocketAddr,
  deadline: Instant,
  attempted: bool,
}

/// What one [`QuicEndpoint::service_dials`] pass produced, for the per-datagram
/// caller to flush and re-index its side effects in the same pass.
///
/// A dial does more than mint bridges: it can create a fresh connection whose
/// `open(Bi)` returns `None` (a cold dial still handshaking, or credit
/// exhausted), or open-and-reset a stream when a `dial_succeeded` intent was
/// invalidated — both mutate a connection (Initial / reset bytes, a rearmed
/// timer) WITHOUT minting a bridge. `touched_conns` lets the caller flush those
/// connections' bytes and refresh their deadline keys the same pass rather than
/// leaving them to an unrelated later tick. The global tick ignores this return
/// (its own `collect_transmits` folds every connection).
struct ServicedDials {
  /// Machine [`StreamId`]s of the outbound bridges MINTED this pass (empty when
  /// none opened). The caller pumps exactly these so each freshly-opened
  /// bridge's first request bytes reach its quinn send stream.
  minted_bridges: SmallVec<StreamId>,
  /// Every distinct connection this pass created or mutated (whether or not it
  /// minted a bridge on it). The caller collects each one's owed transmits and
  /// refreshes its deadline key. First-touch order is preserved so the flushed
  /// datagrams reach `out` in a deterministic sequence; O(1) deduplication while
  /// building it is provided by [`TouchedConns`].
  touched_conns: SmallVec<ConnectionHandle>,
}

/// Accumulates the distinct connections a dial-servicing pass touched, in
/// first-touch order, deduplicated in O(1).
///
/// A dial pass can revisit the same connection many times (several intents to
/// the same pooled peer), so a naive `SmallVec::contains` dedup is O(touched²)
/// across the pass. This pairs an insertion-ordered `SmallVec` — the order the
/// caller flushes connections into `out`, kept stable so the outbound byte
/// sequence is deterministic — with an [`FxHashSet`] for O(1) membership, so the
/// combined dedup is linear in the number of touches.
struct TouchedConns {
  order: SmallVec<ConnectionHandle>,
  seen: FxHashSet<ConnectionHandle>,
}

impl TouchedConns {
  fn new() -> Self {
    Self {
      order: SmallVec::new(),
      seen: FxHashSet::default(),
    }
  }

  /// Record `ch` as touched, preserving first-touch order and ignoring repeats
  /// in O(1).
  fn insert(&mut self, ch: ConnectionHandle) {
    if self.seen.insert(ch) {
      self.order.push(ch);
    }
  }

  /// The distinct touched connections in first-touch order.
  fn into_ordered(self) -> SmallVec<ConnectionHandle> {
    self.order
  }
}

/// Readiness a single [`QuicEndpoint::service_one_conn`] pass observed on its
/// connection that the caller must act on AFTER the connection borrow drops —
/// each unblocks the outbound dials parked on this connection's peer.
///
/// Both marks resolve to the same action: service exactly
/// [`QuicEndpoint::dial_parked`]`[peer]` for this connection's peer, never the
/// whole dial queue. They are surfaced as a return value (not consumed inside
/// `service_one_conn`) because the parked-dial servicing needs the connection
/// borrow released first — it re-borrows `self.conns` through `get_or_dial`.
#[derive(Default)]
struct ServiceMarks {
  /// `true` iff this connection completed its handshake DURING this pass — a
  /// `false -> true` flip of the sticky `established_at_least_once` observation.
  /// A pooled push/pull or reliable-ping intent requeued while the connection
  /// was still handshaking must open its bidi the instant it establishes.
  established_transition: bool,
  /// `Some(peer)` iff this pass drained a `StreamEvent::Available { dir: Bi }` —
  /// the peer raised its MAX_STREAMS bidi limit, so a dial requeued because the
  /// peer's concurrent-bidi-stream credit was exhausted can now open. The peer
  /// address is captured here (rather than re-resolved from the handle) so it
  /// survives the connection being reaped later in the same pass.
  credit_restored_peer: Option<SocketAddr>,
}

/// Coordinator: `memberlist::Endpoint` (unreliable + membership) composed with
/// `quinn_proto::Endpoint` (reliable). Pure Sans-I/O — inject `now`.
///
/// The membership address is pinned to `SocketAddr` inside this coordinator —
/// `quinn_proto::Endpoint` is structurally `SocketAddr`-typed (it dials and
/// accepts wire addresses), so the composed unit pins `A = SocketAddr` rather
/// than carrying a per-coordinator conversion layer over a generic `A`. A
/// driver whose user-facing membership address differs from the wire socket
/// translates at the driver boundary (e.g. in `Memberlist<I, A, R>::join`).
pub struct QuicEndpoint<I, R = SmallRng> {
  ep: Endpoint<I, SocketAddr, R>,
  quinn: QuinnEndpoint,
  cfg: QuicOptions,
  /// Cross-transport compression configuration. A disabled `CompressionOptions`
  /// makes the gossip compress/decompress methods identity.
  #[cfg(compression)]
  compression: crate::CompressionOptions,
  /// Cross-transport encryption configuration. Applied to the QUIC gossip
  /// path (plain UDP on the same socket as the QUIC packets); the QUIC
  /// reliable path always skips — quinn-encrypted streams already provide
  /// confidentiality.
  #[cfg(encryption)]
  encryption: crate::EncryptionOptions,
  /// Checksum configuration for the gossip (unreliable) plane — the QUIC
  /// datagram path. A checksum guards the connectionless datagram path, which
  /// carries no transport-level integrity of its own, so it is applied in
  /// [`Self::checksum_gossip`]. The QUIC reliable bidi bridge carries no
  /// checksum: quinn streams are already integrity-protected end to end, so
  /// corruption detection is an unreliable-plane concern (matching the original
  /// Go memberlist and the legacy port). A disabled `ChecksumOptions` makes the
  /// gossip path identity.
  #[cfg(checksum)]
  checksum: crate::ChecksumOptions,
  /// Cluster label for all reliable bridges spawned by this coordinator, or
  /// `None` when no label is configured.
  ///
  /// Identical source as the gossip-plane label threaded through the driver —
  /// a single `MemberlistOptions::label` feeds both paths so they cannot
  /// diverge. Set via [`Self::with_label`]; defaults to `None` so
  /// constructors that never call `with_label` are byte-identical to before.
  label: Option<bytes::Bytes>,
  /// Forwarded verbatim to each new [`Bridge`]'s `skip_inbound_label_check`
  /// parameter. Suppresses the "label expected but missing from inbound peer"
  /// check without suppressing `DoubleLabel`. Defaults to `false`.
  skip_inbound_label_check: bool,
  conns: ConnTable,
  bridges: HashMap<StreamId, Bridge<I, SocketAddr>>,
  /// Per-connection index of the machine [`StreamId`]s in [`Self::bridges`],
  /// keyed by owning [`ConnectionHandle`]. Maintained alongside every `bridges`
  /// mint and reap so the inbound-datagram servicing path can pump exactly one
  /// connection's bridges in O(that connection's bridges) — never an O(all
  /// bridges) filter over the whole table. A connection with no live bridge has
  /// no entry (the map never stores an empty vec). The index equals the
  /// brute-force `bridges` filtered by `Bridge::ch`; a `#[cfg(test)]` cross-check
  /// asserts that equality after every operation.
  bridges_by_conn: HashMap<ConnectionHandle, SmallVec<StreamId>>,
  /// Pass-scoped queue of bridges made ready THIS servicing pass — the machine
  /// [`StreamId`]s a readiness trigger (a fresh stream mint, or a per-stream
  /// `Readable`/`Writable`/`Finished`/`Stopped` event drained from a
  /// connection's `poll()`) enqueued because owed work appeared. The
  /// per-datagram [`Self::service_connection`] path drains this to pump exactly
  /// the bridges its frames advanced, replacing an O(all bridges on the
  /// connection) pump — so a corrupted or replayed packet, which advances no
  /// stream state and fires no trigger, pumps none. Always drained empty before
  /// a servicing pass returns (a scratch scheduler, never a persistent one): the
  /// global tick's pump-all services every bridge directly and
  /// [`Self::finalize_tick`] clears any residue. Dedup is O(1) via
  /// [`Bridge::queued`]. See [`enqueue_ready_bridge`].
  ready_bridges: VecDeque<StreamId>,
  /// Reverse index from a bridge's `(owning ConnectionHandle, quinn StreamId)`
  /// to its machine [`StreamId`] — the finer-grained twin of
  /// [`Self::bridges_by_conn`]. quinn-proto's per-connection `poll()` yields
  /// `StreamEvent::Finished`/`Stopped` keyed by the quinn `StreamId`, and quinn
  /// sids are per-connection (two pooled peers both hold sid `0`), so routing
  /// needs the compound `(ch, sid)` key. This lets the per-connection servicing
  /// path resolve the owning bridge in O(1) instead of scanning all bridges.
  /// Kept in lockstep with `bridges` at every mint and reap; a `#[cfg(test)]`
  /// cross-check asserts it equals the brute-force `(Bridge::ch, Bridge::sid) ->
  /// id` map after every operation.
  bridge_by_conn_sid: HashMap<(ConnectionHandle, QuicSid), StreamId>,
  /// Incremental count of live INBOUND (server-accepted) bridges — those absent
  /// from [`Self::pending_outbound_kinds`], which is the coordinator's
  /// definition of inbound. Bumped when the `accept(Dir::Bi)` loop mints an
  /// inbound bridge, decremented when any inbound bridge is reaped, so the
  /// cross-connection inbound-stream admission gate reads the population in O(1)
  /// instead of filtering the whole bridge table on every peer-driven
  /// `StreamEvent::Opened`. A `#[cfg(test)]` cross-check asserts it equals that
  /// brute-force filter after every operation.
  inbound_bridge_count: usize,
  /// Tags each outbound bridge's [`StreamId`] with the originating
  /// [`ExchangeKind`] so the bridge-reap path can carry that kind on
  /// the uniform [`Event::ExchangeCompleted`] terminal event. Mirrors
  /// `StreamEndpoint::pending_outbound_kinds`. The reap fires for ALL
  /// outbound kinds (push/pull, reliable ping, reliable user message);
  /// consumers filter on the payload's `kind()` to focus on the
  /// bridges they care about. Populated at `start_push_pull` /
  /// `start_reliable_ping` / `start_user_message` time; drained at
  /// bridge-reap time inside [`Self::emit_exchange_completed`].
  /// Strictly outbound-only — inbound (server-side) bridges accepted
  /// from `streams().accept(Dir::Bi)` are not assigned a kind by the
  /// initiator and never appear in this table.
  pending_outbound_kinds: HashMap<StreamId, ExchangeKind>,
  /// Tags each outbound bridge's [`StreamId`] with the [`SocketAddr`]
  /// of the peer so the bridge-reap path can carry it on the
  /// [`Event::ExchangeCompleted`] payload. Populated alongside
  /// `pending_outbound_kinds`; drained at bridge-reap time inside
  /// [`Self::emit_exchange_completed`]. Inbound (server-side) bridges
  /// do not appear here.
  pending_outbound_peers: HashMap<StreamId, SocketAddr>,
  /// Outbound UDP datagrams produced this tick (quinn datagrams + stateless
  /// `Response`s; the memberlist unreliable path is NOT prebuffered — see
  /// [`poll_memberlist_transmit`](Self::poll_memberlist_transmit)).
  out: VecDeque<(SocketAddr, Bytes)>,
  /// Raw inbound memberlist datagrams the first-byte demux classified as
  /// `Class::Memberlist`. `memberlist-proto` has no umbrella `codec`
  /// dependency, so the coordinator cannot decode them in-crate and MUST NOT
  /// silently drop them (that would lose every UDP ping/ack/alive/suspect on
  /// the composed unit's public ingress). They are buffered here and surfaced
  /// as an explicit action via [`poll_memberlist_ingress`](Self::poll_memberlist_ingress)
  /// — the same idiom the coordinator uses for QUIC `Transmit`/`DatagramEvent`
  /// — for the codec-owning layer to unwrap and feed back through
  /// [`handle_packet`](Self::handle_packet).
  mem_ingress: VecDeque<(SocketAddr, Bytes)>,
  /// Per-peer count of entries currently in `mem_ingress`, maintained on every
  /// push and pop, so the inbound datagram drain can bound one peer's standing
  /// share of the shared queue (fairness against a single-peer flood)
  /// regardless of how many recv passes a driver batches before decoding.
  mem_ingress_per_peer: HashMap<SocketAddr, usize>,
  /// Private queue of pending dial intents. `memberlist::Endpoint::poll_event`
  /// emits `Event::DialRequested { id, peer, deadline }` for an external
  /// driver to dial — but in the composed design `QuicEndpoint` IS the
  /// driver: `service_dials` opens the quinn bidi stream itself, and an
  /// intent whose connection is still handshaking is retried on the next
  /// tick. If `DialRequested` leaked through [`Self::poll_event`] an
  /// external caller draining events between `handle_timeout` and the
  /// next `service_dials` would pop the retry token and silently drop it
  /// — the pending stream intent would orphan and the push/pull or
  /// reliable-ping would never open. The coordinator therefore sieves
  /// `Event::DialRequested` out of the inner endpoint's queue into this
  /// private deque (see [`Self::poll_event`] and [`Self::service_dials`]);
  /// external pollers only ever observe application-visible events.
  ///
  /// This deque holds only FRESH intents — those `service_dials` has not yet
  /// attempted (`attempted == false`). It is a FIFO so first-attempt ordering is
  /// preserved, and a freshly-sieved intent surfaces in [`Self::poll_timeout`]
  /// as an immediate-due wake (see [`PendingDial`]). Once an intent is attempted
  /// and requeues (its connection is still handshaking, or the peer's
  /// concurrent-bidi-stream credit is exhausted) it leaves this deque and parks
  /// in [`Self::dial_parked`], keyed by target peer, so a per-peer readiness
  /// event can service exactly that peer's blocked intents.
  dial_pending: VecDeque<PendingDial>,
  /// Requeued dial intents (`attempted == true`) parked by TARGET PEER, so a
  /// readiness event scoped to one connection — its handshake completing, or its
  /// peer raising MAX_STREAMS — services exactly that peer's blocked intents in
  /// O(that peer's bucket) instead of draining the whole dial queue. An intent
  /// enters here only when its first (or a later) attempt requeues; a bucket is
  /// removed the moment it empties, so a stored bucket is always non-empty and a
  /// missing key means no parked intent for that peer.
  ///
  /// Keyed by the peer `SocketAddr`, NOT the [`ConnectionHandle`], deliberately:
  /// the `peers` map may repoint a peer at a fresh handle while a dial is parked
  /// (a simultaneous dial's `insert_accepted`, or a drain-window redial), and a
  /// peer-keyed bucket survives every such repoint — `get_or_dial` re-resolves
  /// the live handle at attempt time. Establishment of a handle `ch` therefore
  /// services `dial_parked[conns[ch].peer]`.
  ///
  /// [`IndexMap`] (not a plain `HashMap`) so the global tick drains buckets in a
  /// deterministic insertion order, which the conformance simulation relies on.
  dial_parked: IndexMap<SocketAddr, SmallVec<PendingDial>, FxBuildHasher>,
  /// Incremental count of entries in [`Self::dial_pending`] whose `attempted`
  /// bit is `false`. Maintained at every `dial_pending` mutation (enqueue,
  /// `mem::take` drain, requeue) so [`Self::refresh_immediate_due`] answers "is
  /// any dial still unattempted?" in O(1) instead of scanning `dial_pending` on
  /// every `poll_timeout` — which a driver re-polls per inbound receive. A
  /// `#[cfg(test)]` cross-check asserts it equals the brute-force filter after
  /// every operation. Parked (attempted) intents in [`Self::dial_parked`]
  /// contribute zero by construction, so this counter is touched only at the
  /// `dial_pending` FIFO push and drain sites.
  unattempted_dial_count: usize,
  /// Most recent `now: Instant` injected by `handle_udp` / `handle_timeout` /
  /// any high-level `start_*` wrapper. Used by [`Self::poll_timeout`] as the
  /// known-past anchor for the immediate-due wake of an unattempted
  /// `dial_pending` entry: the only way to signal "fire as soon as possible"
  /// out of an `Option<Instant>` Sans-I/O API is to return an `Instant <=
  /// caller's now`, and the only such anchor we may hold is one we observed
  /// from a prior `handle_*` call (Sans-I/O forbids `Instant::now()`). Stays
  /// `None` only before the very first `handle_*` / `start_*` call; after
  /// that, every subsequent `poll_timeout` can return it.
  last_now: Option<Instant>,
  /// Count of unreliable datagrams dropped by
  /// [`queue_unreliable_datagram`](Self::queue_unreliable_datagram) on a quinn
  /// datagram-state error. The `max_size` pre-check excludes `TooLarge`, and
  /// `Blocked` (a full send buffer) is handled separately as a `NotReady`
  /// UDP fallback rather than a drop, so this counts only a residual edge.
  /// Best-effort accounting only — never a membership signal.
  datagram_dropped: u64,
  /// Count of inbound unreliable datagrams the `service_quinn` receive drain
  /// popped from quinn but did NOT push into `mem_ingress`, because either the
  /// per-peer budget ([`MAX_INGRESS_DATAGRAMS_PER_PEER`], one peer's standing
  /// share of the undrained queue) or the node-global cap
  /// ([`MAX_MEM_INGRESS_DATAGRAMS`]) was already reached. quinn's
  /// `datagram_receive_buffer_size` bounds inbound bytes
  /// but not entry count, so a flood of tiny/zero-length DATAGRAM frames is
  /// bounded here by popping-and-dropping past either limit. Best-effort
  /// accounting only — never a membership signal.
  datagram_ingress_dropped: u64,
  /// Incremental earliest-deadline index backing [`Self::poll_timeout`]. Holds
  /// the next deadline for every connection, bridge, and pending dial (plus the
  /// membership endpoint and the immediate-due anchor) so `poll_timeout` reads
  /// the unified minimum in amortized O(1) instead of folding
  /// O(connections + bridges + dials) per call. Every deadline-mutating site
  /// updates the relevant [`TimerKey`]; the invariant is cross-checked in tests
  /// against a brute-force fold of the same sources.
  deadline_index: DeadlineIndex,
  /// Connections that currently hold a deferred `ConnectionEvent` backlog
  /// (queued by `service_quinn` for delivery on the connection's next
  /// iteration — see [`conn::ConnEntry::queue_pending_event`]). Maintained
  /// incrementally at the queue/drain sites so the immediate-due aggregate can
  /// answer "does any connection have pending events?" in O(1) without scanning
  /// the connection table on every `poll_timeout`.
  conns_with_pending_events: HashSet<ConnectionHandle>,
  /// Test-only instrumentation counters — one per negative-control regression
  /// test; see [`TestCounters`] for the per-counter contract. Never compiled
  /// into production builds.
  #[cfg(test)]
  counters: TestCounters,
}

/// Test-only instrumentation for [`QuicEndpoint`] — one counter per
/// negative-control regression test. Never compiled into production builds.
#[cfg(test)]
#[derive(Debug, Default)]
struct TestCounters {
  /// Test-only counter incremented once per `EndpointEvent` drained from
  /// every connection's `poll_endpoint_events()` queue inside
  /// [`QuicEndpoint::service_quinn`]. Exists ONLY for the negative-control regression
  /// test that proves the endpoint-event drain loop runs at all (a missing
  /// drain leaves the counter at zero and breaks CID issuance / reset-token
  /// registration in quinn-proto — see [`QuicEndpoint::service_quinn`] for the
  /// per-event contract). Never compiled into production builds.
  endpoint_events_processed: u64,
  /// Test-only counter incremented once per [`Endpoint::handle_timeout`] call,
  /// i.e. once per membership-time advance. Exists ONLY for the regression test
  /// proving a QUIC packet ingress ([`QuicEndpoint::service_connection`]) does NOT
  /// advance membership time — only the driver's explicit `handle_timeout` does —
  /// so a probe Ack carried in a datagram cannot be timed out before it is decoded.
  /// Never compiled into production builds.
  membership_time_advances: u64,
  /// Test-only counter incremented once per bridge `drain_then_reap`'d
  /// inside [`QuicEndpoint::service_quinn`] on an `Event::ConnectionLost` — the
  /// strict-poll self-sufficiency path that closes the D1 drain within
  /// the SAME tick the loss is observed (rather than deferring to a
  /// future `pump_bridges` that a strict-poll driver may never wake to
  /// run on a quiet cluster). The negative-control regression test asserts
  /// this counter advances under strict poll-surface driving; reverting
  /// the inline drain to mere `mark_fatal()` leaves it at zero. Never
  /// compiled into production builds.
  bridges_reaped_on_connection_lost: u64,
  /// Test-only counter incremented once per bridge pumped post-acceptance —
  /// by the second `pump_bridges` invocation in [`QuicEndpoint::run_tick`]
  /// (step 5.5) / [`QuicEndpoint::flush_outbound`], OR by the per-connection
  /// [`QuicEndpoint::service_connection`] datagram path — that was inserted into
  /// `self.bridges` by an `accept(Dir::Bi)` loop or `service_dials`'s
  /// `open(Dir::Bi)` DURING the same servicing pass, AFTER the pass's initial
  /// bridge pump already ran. A newly-inserted inbound bridge
  /// carries its first buffered request data inside quinn's per-stream recv
  /// buffer (delivered by the inbound datagram `service_quinn` just
  /// ingested); a newly-opened outbound bridge carries its first request
  /// bytes in its FSM `Stream` output buffer. Without the second pump,
  /// `Bridge::pump_in` / `Bridge::pump_out` never run on those bridges this
  /// tick, and a strict-poll driver next wakes at the bridge's exchange
  /// deadline — at which point `Stream::handle_data` rejects the buffered
  /// request as timed out and the exchange fails. Counter advances ONLY
  /// when at least one such bridge was pumped post-acceptance; the
  /// regression test asserts it advances under strict poll-surface
  /// driving and reverting step (5.5) leaves it at zero. Never compiled
  /// into production builds.
  bridges_pumped_after_acceptance: u64,
  /// Test-only counter incremented once each time [`QuicEndpoint::route_datagram_event`]
  /// surfaces an `AcceptError::response` from `quinn_proto::Endpoint::accept`
  /// onto the driver-facing `out` queue. quinn-proto attaches an
  /// `Option<Transmit>` to its `AcceptError` whenever `accept` owes a
  /// refusal/close to the peer (CID exhaustion or a handshake
  /// `TransportError` produce an `initial_close` response). The close bytes
  /// are written into the caller-supplied `buf` and the returned
  /// `Transmit.size` equals `buf.len()`; without this counter we have no
  /// observable seam to assert the refusal/close `Transmit` actually reaches
  /// the driver via the `out` queue. Never compiled into production builds.
  accept_error_responses_emitted: u64,
  /// Test-only counter incremented each time `pump_bridges`'s
  /// post-`drain_payload_only` `is_terminal()` re-check fires — i.e. a
  /// bridge that was non-terminal entering `drain_payload_only` became
  /// terminal during the per-tick endpoint-event drain (typically a
  /// `StreamCommand::Close` from a `MergeDelegate` / `AliveDelegate`
  /// rejection sets `fatal`). Provides an observable seam for the
  /// admission-rejection same-tick reap because `live_bridge_count`
  /// would otherwise show the bridge transiently — appearing on accept
  /// then immediately reaping in the same tick. Never compiled into
  /// production builds.
  bridges_terminalized_via_close_command: u64,
  /// Test-only counter incremented once per [`QuicEndpoint::service_connection`]
  /// call — i.e. once per inbound QUIC datagram that
  /// [`route_datagram_event`](QuicEndpoint::route_datagram_event) resolved to a
  /// connection to service (a `NewConnection` accepted into the table, or a
  /// `ConnectionEvent` for a live handle). A datagram quinn discards (`handle` →
  /// `None`), a stateless `Response`, an over-cap or failed `accept`, and a
  /// `ConnectionEvent` for an unknown handle all resolve to no connection and are
  /// NOT counted. The negative-control regression tests assert this counter stays
  /// flat on such datagrams yet advances on a real accept or live-connection
  /// event; servicing on every `handle` result (or unconditionally) makes it
  /// advance on the inert cases and those tests fail. Never compiled into
  /// production builds.
  quic_inbound_servicings: u64,
  /// Test-only counter of connections *touched* by a servicing pass — bumped once
  /// per [`QuicEndpoint::service_one_conn`] entry, whether from the global
  /// `service_quinn` loop or the per-connection [`QuicEndpoint::service_connection`]
  /// datagram path. The visit-count proofs reset it before one `handle_udp` and
  /// assert the per-datagram delta stays O(1) — exactly the addressed connection,
  /// never scaling with the table size. Restoring the global tick on the datagram
  /// path makes the delta scale with the connection count and those tests fail.
  /// Never compiled into production builds.
  connection_visits: u64,
  /// Test-only counter of bridges *touched* by a servicing pass — bumped once per
  /// [`QuicEndpoint::pump_one_bridge`] entry that finds the bridge present,
  /// whether from the global `pump_bridges` loop or the per-connection
  /// [`QuicEndpoint::drain_ready_bridges`] datagram path. Paired with
  /// [`Self::connection_visits`] in the visit-count proofs: a datagram pumps
  /// only the bridges a readiness trigger enqueued this pass, so a corrupted or
  /// replayed packet (which fires no trigger) pumps zero and the per-datagram
  /// delta never scales with the total bridge population. Never compiled into
  /// production builds.
  bridge_visits: u64,
  /// Test-only high-water mark of the concurrent INBOUND bridge population,
  /// sampled inside the `accept(Dir::Bi)` loop at each mint (so it captures the
  /// true peak even when a short exchange accepts and reaps its bridge within the
  /// same tick — which a between-tick `live_bridge_count()` sample would miss).
  /// The inbound-cap regression test asserts this equals the ceiling: an
  /// off-by-one admits ceiling+1, admit-all drives it to the opened count,
  /// reject-all leaves it at 0. Never compiled into production builds.
  max_inbound_bridges_live: usize,
  /// Test-only counter incremented once each time the `NewConnection` admission
  /// path performs its per-source pending-index lookup — i.e. once per inbound
  /// Initial that reached the per-source check under the global cap. The global
  /// cap is checked FIRST and short-circuits, so an Initial refused at
  /// connection-table saturation must NOT advance this counter. The
  /// global-first regression test asserts it stays flat at saturation; reverting
  /// to the eager `over_global || over_per_source` form (which computes the
  /// per-source lookup unconditionally) makes it advance and that test fails.
  /// Never compiled into production builds.
  quic_pending_inbound_checks: u64,
  /// Test-only count of `self.bridges` entries EXAMINED while routing a
  /// per-connection stream event (`StreamEvent::Finished`/`Stopped`) or reaping
  /// a lost connection's bridges inside [`QuicEndpoint::service_one_conn`].
  /// After the incremental-index redesign these operations resolve the owning
  /// bridge via [`QuicEndpoint::bridge_by_conn_sid`] (O(1)) or iterate only the
  /// connection's own entries via [`QuicEndpoint::bridges_by_conn`]
  /// (O(that connection's bridges)), so the per-datagram delta does not scale
  /// with the total bridge population. The visit-count regression test resets
  /// this before triggering one such event and asserts the delta stays bounded
  /// by the addressed connection's bridge count; restoring a global
  /// `self.bridges` scan makes it climb to the total and the test fails. Never
  /// compiled into production builds.
  bridge_scan_visits: u64,
  /// Test-only count of per-element SmallVec surgeries performed while removing a
  /// reaped bridge from its [`QuicEndpoint::bridges_by_conn`] bucket. Bumped once
  /// per single-bridge [`QuicEndpoint::index_bridge_reap`] `swap_remove` (an O(1)
  /// single-element relocation); the connection-loss path takes the whole bucket
  /// in one map remove and runs a bucketless deindex, so it never bumps this. A
  /// K-bridge burst therefore costs O(K). Reverting the removal to the former
  /// `retain` (which scans the whole bucket per reap, counting each scanned
  /// element) makes it climb to O(K²) across the burst, which the visit-count
  /// regression tests assert against. Never compiled into production builds.
  bridge_bucket_scan_ops: u64,
  /// Test-only count of single-bridge pump-path reaps — bumped once per
  /// [`QuicEndpoint::deindex_reaped_bridge`] call, the one path that pairs a
  /// bucketless deindex with an O(1) bucket `swap_remove`. The invariant
  /// [`Self::bridge_bucket_scan_ops`] `==` this holds by construction (each such
  /// reap does exactly one swap_remove), independent of how many bridges reap in
  /// a run. Reverting the removal to the former O(bucket) `retain` breaks the
  /// equality (scan ops climb above the reap count); the sibling-reap regression
  /// test asserts it. Never compiled into production builds.
  bridge_pump_path_reaps: u64,
  /// Test-only negative-control counter of `dial_pending` entries EXAMINED by
  /// [`QuicEndpoint::refresh_immediate_due`] (the `poll_timeout` hot path). The
  /// shipped O(1) path reads [`QuicEndpoint::unattempted_dial_count`] and never
  /// iterates `dial_pending`, so this stays `0` no matter how many dials are
  /// parked; the Finding-3 regression test asserts that across repeated
  /// `poll_timeout` calls. Reverting `refresh_immediate_due` to the
  /// `dial_pending.iter().any(..)` scan (counting each visited entry) makes it
  /// scale with the parked-dial count and the test fails. Never compiled into
  /// production builds.
  dial_pending_scan_visits: u64,
  /// Test-only count of dial intents EXAMINED by [`QuicEndpoint::process_dial_entry`]
  /// — one per entry drained from the fresh FIFO or a parked bucket, whichever
  /// path fed it. The scoped establishment / credit-restore path services only
  /// the ready peer's own bucket, so a single connection establishing bumps this
  /// by exactly that peer's parked-intent count, never the whole dial
  /// population. The visit-count regression test resets it before the
  /// establishing datagram and asserts the delta equals the ready peer's bucket
  /// size; reverting the establishment path to the whole-queue `service_dials`
  /// drain makes it scale with every parked dial and the test fails. Never
  /// compiled into production builds.
  dial_entries_serviced: u64,
}

// Construction, transform configuration, transport plumbing, and accessors —
// methods whose bodies touch only non-generic fields or delegate to an
// `Endpoint` accessor that needs no node identity. No bound required.
impl<I, R> QuicEndpoint<I, R> {
  /// Build the coordinator. The quinn endpoint is created with the bundled
  /// config; `allow_mtud = true`, and `rng_seed = None` so quinn seeds its
  /// connection-ID / path-challenge RNG from the OS (production entropy).
  ///
  /// Signature (quinn-proto 0.11.14): `Endpoint::new(Arc<EndpointOptions>,
  /// Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)`.
  pub fn new(ep: Endpoint<I, SocketAddr, R>, cfg: QuicOptions) -> Self {
    Self::with_quinn_rng_seed(ep, cfg, None)
  }

  /// Build the coordinator with an explicit quinn `rng_seed`.
  ///
  /// `rng_seed` is quinn's documented determinism seam: it seeds the
  /// endpoint's connection-ID generator and path-challenge RNG. Production
  /// uses [`new`](Self::new) (`None` → OS entropy); a deterministic driver
  /// (e.g. the conformance simulation, whose temporal determinism is the
  /// injected virtual clock) passes a fixed `Some([_; 32])` so the QUIC
  /// transport — and therefore the composed membership behaviour and timing
  /// — is bit-for-bit reproducible across runs. Behaviour is otherwise
  /// identical to [`new`](Self::new).
  #[must_use]
  pub fn with_quinn_rng_seed(
    ep: Endpoint<I, SocketAddr, R>,
    cfg: QuicOptions,
    rng_seed: Option<[u8; 32]>,
  ) -> Self {
    let quinn = QuinnEndpoint::new(cfg.endpoint_arc(), Some(cfg.server_arc()), true, rng_seed);
    Self {
      ep,
      quinn,
      cfg,
      #[cfg(compression)]
      compression: crate::CompressionOptions::new(),
      #[cfg(encryption)]
      encryption: crate::EncryptionOptions::new(),
      #[cfg(checksum)]
      checksum: crate::ChecksumOptions::new(),
      label: None,
      skip_inbound_label_check: false,
      conns: ConnTable::new(),
      bridges: HashMap::new(),
      bridges_by_conn: HashMap::new(),
      ready_bridges: VecDeque::new(),
      bridge_by_conn_sid: HashMap::new(),
      inbound_bridge_count: 0,
      pending_outbound_kinds: HashMap::new(),
      pending_outbound_peers: HashMap::new(),
      out: VecDeque::new(),
      mem_ingress: VecDeque::new(),
      mem_ingress_per_peer: HashMap::new(),
      dial_pending: VecDeque::new(),
      dial_parked: IndexMap::default(),
      unattempted_dial_count: 0,
      last_now: None,
      datagram_dropped: 0,
      datagram_ingress_dropped: 0,
      deadline_index: DeadlineIndex::new(),
      conns_with_pending_events: HashSet::new(),
      #[cfg(test)]
      counters: TestCounters::default(),
    }
  }

  /// Build the coordinator with an explicit cross-transport compression
  /// configuration. [`Self::new`] is `with_compression` with compression
  /// disabled.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  #[must_use]
  pub fn with_compression(
    ep: Endpoint<I, SocketAddr, R>,
    cfg: QuicOptions,
    compression: crate::CompressionOptions,
  ) -> Self {
    let mut this = Self::new(ep, cfg);
    this.compression = compression;
    this
  }

  /// Attach a cluster label to this coordinator. Every reliable bridge opened
  /// after this call inherits `label` and `skip_inbound_label_check`.
  ///
  /// `label = None` — or an empty label, which normalizes to `None` — is
  /// byte-identical to having never called this builder: no label frame is
  /// written and the inbound path skips validation entirely. An over-long
  /// (> 253-byte) or non-UTF-8 label returns
  /// [`LabelError`](crate::label::LabelError). The intended call site is the
  /// driver constructor, which threads the same `MemberlistOptions::label`
  /// value used by the gossip codec so the two planes share one source and
  /// cannot diverge.
  pub fn with_label(
    mut self,
    label: Option<bytes::Bytes>,
    skip_inbound_label_check: bool,
  ) -> Result<Self, crate::label::LabelError> {
    // Normalize and validate at this public entry: an empty label collapses to
    // the byte-identical no-label path, and an over-long or non-UTF-8 label is
    // rejected here so it can never reach `encode_label_prefix`, which would
    // truncate the single length byte and let the overflow be parsed as
    // reliable-unit data.
    self.label = match label {
      None => None,
      Some(bytes) if bytes.is_empty() => None,
      Some(bytes) => {
        crate::label::validate_label(&bytes)?;
        Some(bytes)
      }
    };
    self.skip_inbound_label_check = skip_inbound_label_check;
    Ok(self)
  }

  /// The configured cluster label, or `None` for an unlabeled coordinator.
  #[cfg(test)]
  pub(crate) fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// The configured cross-transport compression options.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  pub fn compression(&self) -> crate::CompressionOptions {
    self.compression
  }

  /// Reconfigure the gossip compression policy in place. Applies to
  /// the next outbound datagram.
  ///
  /// QUIC's reliable path is skipped — quinn streams provide
  /// confidentiality and integrity intrinsically, so compression is
  /// applied only to the gossip path (plain UDP datagrams sharing
  /// the same socket as the QUIC packets).
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  pub fn set_compression_options(&mut self, compression: crate::CompressionOptions) {
    self.compression = compression;
  }

  /// Compress one outbound gossip datagram for the wire. When compression is
  /// disabled, or the datagram does not benefit, the original bytes are
  /// returned.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  pub fn compress_gossip(&self, datagram: &[u8]) -> Vec<u8> {
    match self.compression.apply(datagram) {
      Ok(crate::CompressionOutput::Compressed(packed)) => {
        let wrapped = crate::encode_compressed_frame(
          self
            .compression
            .algorithm()
            .expect("a Compressed outcome implies an algorithm is set"),
          datagram.len(),
          &packed,
        );
        // The wrapper header (tag + algorithm + `orig_len` varint) is overhead
        // on top of the raw compressed bytes; if it pushes the wrapped
        // datagram to `datagram`'s size or larger, send `datagram` plain so
        // compressed gossip can never inflate. The receiver's
        // `unwrap_transforms` passes a non-wrapper buffer through unchanged.
        if wrapped.len() < datagram.len() {
          wrapped
        } else {
          datagram.to_vec()
        }
      }
      // Plain outcome, or a backend error: emit the datagram uncompressed.
      _ => datagram.to_vec(),
    }
  }

  /// The configured gossip-plane checksum options. Applies to the QUIC
  /// datagram (unreliable) path only; the reliable bidi path carries no
  /// checksum.
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  pub fn checksum(&self) -> crate::ChecksumOptions {
    self.checksum
  }

  /// Reconfigure the gossip-plane checksum policy in place. Applies to the next
  /// outbound gossip datagram via [`Self::checksum_gossip`]. The reliable bidi
  /// bridge carries no checksum (quinn provides its own integrity), so there is
  /// no per-bridge fan-out.
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  pub fn set_checksum_options(
    &mut self,
    checksum: crate::ChecksumOptions,
  ) -> Result<(), crate::ChecksumError> {
    // Validate the algorithm's backend is built into this binary BEFORE storing
    // it: an unusable policy would make every subsequent `checksum_gossip` fail
    // and the driver drop the datagram — silently disabling all gossip behind a
    // false success. The empty-payload probe surfaces `Disabled` /
    // `UnknownAlgorithm`.
    checksum.apply(&[])?;
    self.checksum = checksum;
    Ok(())
  }

  /// Wrap one outbound gossip datagram in a checksum frame for the wire. The
  /// codec-owning driver calls this on the already-compressed gossip bytes
  /// (from [`Self::compress_gossip`]) BEFORE [`Self::encrypt_gossip`], so the
  /// on-wire order is `[Encrypted[Checksumed[Compressed[frame]]]]`. When
  /// checksumming is disabled the bytes are returned unchanged.
  ///
  /// Returns `Err` when a checksum algorithm is configured but its backend was
  /// not built into this binary; the driver MUST drop the gossip rather than
  /// emit an unverifiable datagram, mirroring [`Self::encrypt_gossip`].
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  pub fn checksum_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::ChecksumError> {
    match self.checksum.apply(datagram)? {
      crate::ChecksumOutput::Checksumed(framed) => Ok(framed),
      crate::ChecksumOutput::Plain => Ok(datagram.to_vec()),
    }
  }

  /// The configured cross-transport encryption options. Applies to the
  /// gossip path only; the QUIC reliable path always skips.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  pub fn encryption_options(&self) -> &crate::EncryptionOptions {
    &self.encryption
  }

  /// Encrypt one outbound gossip datagram for the wire. The codec-owning
  /// driver calls this on the already-compressed-and-checksummed gossip bytes
  /// (from [`Self::compress_gossip`] → [`Self::checksum_gossip`]) before
  /// handing them to the UDP socket. When encryption is disabled the bytes
  /// are returned unchanged.
  ///
  /// The on-wire byte order with the full stack is
  /// `[Encrypted[Checksumed[Compressed[frame]]]]`; each disabled layer
  /// collapses to identity (e.g. `[Encrypted[frame]]` with compression and
  /// checksum off).
  ///
  /// Returns `Err` when encryption is configured but the backend rejects the
  /// request — typically [`crate::EncryptionError::UnsupportedAlgorithm`]
  /// for a primary key whose backend was not built into this binary. The
  /// driver MUST drop the gossip in that case; emitting unencrypted bytes
  /// on an encrypted-cluster path would bypass authentication silently.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  pub fn encrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::EncryptionError> {
    let keyring = match self.encryption.keyring() {
      Some(kr) => kr,
      None => return Ok(datagram.to_vec()),
    };
    let key = keyring.primary_ref();
    crate::encode_encrypted_frame(key.algorithm(), key, datagram)
  }

  /// Unwrap one inbound gossip datagram. The codec-owning driver calls this
  /// on the raw bytes from [`Self::poll_memberlist_ingress`] BEFORE decoding
  /// frames — it strips the Encrypted-then-Checksumed-then-Compressed wrapper
  /// stack in one pass (each layer identity when its wrapper is absent, the
  /// checksum layer verifying the digest as it strips). A datagram with no
  /// Encrypted wrapper is returned unchanged when no keyring is configured;
  /// when a keyring IS configured the strict-mode entry check rejects any
  /// non-Encrypted leading tag. A corrupt or unknown-algorithm wrapper, a
  /// checksum mismatch, or a frame the keyring cannot decrypt, is an `Err` —
  /// the driver drops the datagram (gossip is lossy and self-healing).
  ///
  /// This is the SINGLE canonical ingress unwrap on the coordinator. The
  /// outbound side uses [`Self::compress_gossip`] → [`Self::checksum_gossip`]
  /// → [`Self::encrypt_gossip`] so the on-wire order is
  /// `[Encrypted[Checksumed[Compressed[frame]]]]`; this helper reverses all
  /// layers, so authentication and integrity never depend on integration
  /// discipline.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  pub fn decrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::FrameError> {
    // Ceiling is the gossip MTU — the maximum size any compliant gossip
    // datagram decompresses to. A wrapper claiming more is not a compliant
    // datagram and is rejected. The encryption-aware unwrap consumes an
    // Encrypted wrapper through the keyring, then verifies and strips a
    // Checksumed wrapper, then strips a Compressed wrapper if present; a
    // non-Encrypted-led datagram is returned unchanged when no keyring is
    // configured (the strict-mode entry check is gated on
    // `encryption.is_enabled()`).
    crate::unwrap_transforms_with_encryption(datagram, self.ep.gossip_mtu(), &self.encryption)
      .map(|cow| cow.into_owned())
  }

  /// Borrow the inner membership endpoint (members / queue_user_broadcast / …).
  #[inline(always)]
  pub fn endpoint_ref(&self) -> &Endpoint<I, SocketAddr, R> {
    &self.ep
  }

  /// Install a custom peer-admission predicate. Forwards to
  /// [`Endpoint::set_alive_delegate`]; the machine consults it inline for every
  /// inbound Alive (gossip and join push/pull).
  #[inline]
  pub fn set_alive_delegate(
    &mut self,
    delegate: impl crate::delegate::AliveDelegate<I, SocketAddr>,
  ) {
    self.ep.set_alive_delegate(delegate);
  }

  /// Install a custom join-merge predicate. Forwards to
  /// [`Endpoint::set_merge_delegate`]; the machine consults it on each join
  /// push/pull merge.
  #[inline]
  pub fn set_merge_delegate(
    &mut self,
    delegate: impl crate::delegate::MergeDelegate<I, SocketAddr>,
  ) {
    self.ep.set_merge_delegate(delegate);
  }

  /// The machine's load-shedding counters for this QUIC endpoint. Folds the QUIC
  /// datagram-plane ingress shed (`datagram_ingress_dropped`) into
  /// [`gossip_ingress_dropped`](crate::metrics::Metrics::gossip_ingress_dropped)
  /// — the inner `Endpoint`'s own gossip-ingress count is the STREAM plane's
  /// (zero on a QUIC endpoint) — so a driver reads one unified counter regardless
  /// of transport.
  pub fn metrics(&self) -> crate::metrics::Metrics {
    // `Endpoint::metrics` returns a borrow; `Metrics` is `Copy`, so take an
    // owned copy to fold in this endpoint's datagram-plane shed count.
    let mut m = *self.ep.metrics();
    m.gossip_ingress_dropped = m
      .gossip_ingress_dropped
      .saturating_add(self.datagram_ingress_dropped);
    m
  }

  /// Mutably borrow the inner membership endpoint — test-only. Production
  /// code accesses `self.ep` directly inside `QuicEndpoint`'s own methods.
  /// A public raw `&mut Endpoint` would let external callers drain
  /// `Event::DialRequested` directly out of the inner queue (via
  /// `endpoint_mut().poll_event()`) and orphan the `PendingStreamIntent` —
  /// the QUIC bridge would never open, the immediate-due `poll_timeout`
  /// term would never fire, and the exchange (push/pull, reliable-ping
  /// fallback, user-message) would silently strand. External callers go
  /// through scoped pass-through methods ([`Self::start_push_pull`],
  /// [`Self::start_reliable_ping`], [`Self::start_user_message`],
  /// [`Self::start_probe`], [`Self::handle_alive`], [`Self::requeue_event`])
  /// AND the sieving public [`Self::poll_event`], preserving the sealed
  /// inner endpoint invariant that no caller can drain `DialRequested`
  /// out from under `service_dials`.
  #[cfg(test)]
  pub(crate) fn endpoint_mut(&mut self) -> &mut Endpoint<I, SocketAddr, R> {
    &mut self.ep
  }

  /// Returns `true` if the endpoint is in normal operation (not leaving
  /// or left). Forwards to [`Endpoint::is_running`]. A driver consults
  /// this before calling [`Self::leave`] to distinguish a leave that
  /// actually initiates the dead-self flush (and will emit
  /// [`Event::LeftCluster`](crate::event::Event::LeftCluster)) from an
  /// idempotent post-leave no-op (which will not).
  #[inline]
  pub fn is_running(&self) -> bool {
    self.ep.is_running()
  }

  /// The reliable-stream frame ceiling
  /// ([`max_stream_frame_size`](crate::config::EndpointOptions::max_stream_frame_size)).
  /// The driver derives its observation-channel payload byte budget from this.
  #[inline]
  pub fn max_stream_frame_size(&self) -> usize {
    self.ep.max_stream_frame_size()
  }
  /// Next outbound UDP datagram (quinn or encoded memberlist), if any.
  pub fn poll_transmit(&mut self) -> Option<(SocketAddr, Bytes)> {
    self.out.pop_front()
  }

  /// Next raw inbound memberlist datagram (the first-byte demux classified it
  /// `Class::Memberlist`), if any.
  ///
  /// `memberlist-proto` has no umbrella `codec` dependency, so the
  /// coordinator cannot perform the structured unwrap
  /// (label → decrypt → decompress → split-compound) in-crate; it surfaces
  /// the raw `(from, bytes)` as an explicit action instead of silently
  /// dropping it (a silent drop would lose every UDP ping/ack/alive/suspect
  /// on the composed unit's public ingress). The codec-owning layer drains
  /// this, decodes each `Message`, and feeds it back through
  /// [`handle_packet`](Self::handle_packet).
  pub fn poll_memberlist_ingress(&mut self) -> Option<(SocketAddr, Bytes)> {
    let (from, bytes) = self.mem_ingress.pop_front()?;
    // Keep the per-peer share counter exact: decrement on pop and remove the
    // entry at zero so no stale zeros accumulate (one map key per peer with a
    // live standing share, nothing more).
    if let std::collections::hash_map::Entry::Occupied(mut slot) =
      self.mem_ingress_per_peer.entry(from)
    {
      let n = slot.get_mut();
      *n -= 1;
      if *n == 0 {
        slot.remove();
      }
    }
    Some((from, bytes))
  }

  /// Number of live (non-reaped) QUIC connections to `peer` — `0` or `1`,
  /// since the connection table pools one connection per peer.
  ///
  /// Observation-only, for a driver/test to assert the drained-reap
  /// lifecycle (a connection that idled past `max_idle_timeout` is reaped:
  /// its slab + peers entry is removed, so this drops back to `0`). A
  /// connection still in its closing/draining wind-down is reported live
  /// until [`ConnTable::reap_if_drained`] removes it.
  pub fn live_connections_to(&self, peer: SocketAddr) -> usize {
    match self.conns.handle_for(&peer) {
      Some(ch) => usize::from(
        self
          .conns
          .get(ch)
          .map(|e| !e.conn_ref().is_drained())
          .unwrap_or(false),
      ),
      None => 0,
    }
  }

  /// Number of active reliable-exchange bridges (one per in-flight push/pull
  /// or reliable-ping stream). Observation-only, for a driver/test to assert
  /// no bridge leaked after an exchange completed or its connection dropped.
  pub fn live_bridge_count(&self) -> usize {
    self.bridges.len()
  }

  /// The configured plaintext-byte ceiling for an outbound gossip datagram.
  /// Sourced from [`crate::config::EndpointOptions::gossip_mtu`] (default
  /// [`crate::config::DEFAULT_GOSSIP_MTU`]). The on-wire datagram may
  /// exceed this by [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] when
  /// encryption is enabled.
  pub fn gossip_mtu(&self) -> usize {
    self.ep.gossip_mtu()
  }

  /// Probe the protocol-layer credit for opening a remote-initiated
  /// unidirectional stream to `peer`. Returns `true` iff the open
  /// would have succeeded; on the (rare) success branch the probe
  /// CLOSES THE ENTIRE CONNECTION before returning so no hidden
  /// stream state can persist on a reusable connection.
  ///
  /// Diagnostic only: the composed unit disables remotely-initiated
  /// unidirectional streams by construction — the transport config
  /// installed by [`QuicOptions::new`] advertises
  /// `max_concurrent_uni_streams = 0`, so on a peer that observed
  /// our transport parameters this method MUST return `false` once
  /// the handshake completes. A test can use this to assert that
  /// the protocol-layer refusal is in effect; it is not a path the
  /// coordinator itself uses (all coordinator-initiated streams
  /// are bidirectional).
  ///
  /// Why the close-on-success — `quinn_proto::Streams::open(Dir::Uni)`
  /// inserts send state and increments `StreamsState::send_streams`.
  /// `SendStream::reset(0)` only marks the stream `ResetSent` and
  /// queues a `RESET_STREAM` frame; the entry is freed on the peer's
  /// reset ACK, NOT synchronously. A `true` branch therefore means
  /// (a) the transport-config invariant was violated (an unsafe state
  /// to keep using the connection), AND (b) any reset-only retirement
  /// would leave hidden state on the pooled connection until the peer
  /// ACKs the reset. `Connection::close(now, 0, empty)` tears down the
  /// connection-level state immediately (transitions to `State::Closed`,
  /// queues `CONNECTION_CLOSE` once, marks `is_drained()` after the next
  /// `poll_transmit`/`poll_timeout` cycle); the coordinator's
  /// `finalize_tick` then drained-reaps the slab + peers entry.
  /// `last_now` is also anchored so any wake the close requires
  /// surfaces immediately.
  ///
  /// Returns `false` if no connection to `peer` exists, or if the
  /// open is refused.
  pub fn try_open_uni_stream_to(&mut self, peer: SocketAddr, now: Instant) -> bool {
    self.last_now = Some(now);
    let Some(ch) = self.conns.handle_for(&peer) else {
      return false;
    };
    let Some(e) = self.conns.get_mut(ch) else {
      return false;
    };
    let opened = e.conn_mut().streams().open(Dir::Uni).is_some();
    if opened {
      e.conn_mut().close(
        now.into_std(),
        quinn_proto::VarInt::from_u32(0),
        bytes::Bytes::new(),
      );
    }
    // `open`/`close` rearmed the connection's timers and no servicing tick
    // follows this call, so refresh its deadline key inline.
    let deadline = e.conn_mut().poll_timeout().map(crate::Instant::from_std);
    self.deadline_index.set(TimerKey::Conn(ch), deadline);
    opened
  }
  /// Build the coordinator with an explicit cross-transport encryption
  /// configuration. [`Self::new`] is `with_encryption` with encryption
  /// disabled. The configuration applies to the QUIC gossip (plain UDP)
  /// path only; the QUIC reliable path always skips encryption because
  /// quinn-encrypted streams already provide confidentiality.
  ///
  /// Routes through [`Self::set_encryption_options`] so the bridge-fan-out
  /// runs in both the builder and the in-place setter, matching
  /// [`crate::streams::StreamEndpoint::with_encryption`].
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  #[must_use]
  pub fn with_encryption(mut self, encryption: crate::EncryptionOptions) -> Self {
    self.set_encryption_options(encryption);
    self
  }

  /// Replace the encryption options in place. The driver calls this on a key
  /// rotation: build a new `EncryptionOptions` with the rotated `Keyring`,
  /// then publish it via the setter. Single-threaded `&mut self` — no lock.
  ///
  /// Propagates the new options to every live reliable bridge for symmetry
  /// with the plain-stream coordinator (see
  /// [`crate::streams::StreamEndpoint::set_encryption_options`]). On QUIC the
  /// reliable bridge always force-disables encryption (quinn already
  /// encrypts the stream), so the bridge-level propagation is a no-op — the
  /// gossip path's strictness propagates immediately via the coordinator's
  /// own `self.encryption` field (`decrypt_gossip` reads it directly each
  /// call).
  ///
  /// **No-op reapply** — short-circuits at entry if the new options equal
  /// the current ones, mirroring [`crate::streams::StreamEndpoint::set_encryption_options`]'s
  /// own guard. The bridge-fan-out is a no-op on QUIC, but the
  /// `bridge.set_encryption(encryption.clone())` clone-and-call still
  /// runs once per live bridge — pure waste on a config reconciler that
  /// republishes the same effective policy.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  pub fn set_encryption_options(&mut self, encryption: crate::EncryptionOptions) {
    if self.encryption == encryption {
      self.encryption = encryption;
      return;
    }
    for bridge in self.bridges.values_mut() {
      bridge.set_encryption(encryption.clone());
    }
    // Drop every buffered raw gossip datagram. `handle_memberlist_udp`
    // enqueues `(src, raw_bytes)` into [`Self::mem_ingress`];
    // [`Self::decrypt_gossip`] reads the coordinator's CURRENT
    // `self.encryption` at drain time. Without this clear, a datagram queued
    // before the policy change is decrypted under the NEW policy — a
    // plaintext datagram queued while strict-mode was ON would be accepted
    // after the operator switches to disabled, and a ciphertext datagram
    // queued while disabled would be rejected after enabling. Gossip is
    // lossy and self-healing, so the dropped bytes recover on the next
    // gossip round. The QUIC reliable bridges force-disable encryption
    // regardless of the new options (quinn already encrypts the stream), so
    // there is no per-bridge failure path here — the gossip ingress buffer
    // is the only at-risk queue on policy change.
    self.mem_ingress.clear();
    // Keep the per-peer share counter consistent with the now-empty queue.
    self.mem_ingress_per_peer.clear();
    self.encryption = encryption;
  }

  /// Re-queue an event for observation by a later [`Self::poll_event`]
  /// (the sieving public drain).
  ///
  /// Anchors `last_now = Some(now)` unconditionally — the immediate-due
  /// `poll_timeout` rescue for an unattempted `dial_pending` entry
  /// requires `last_now` to be `Some(...)`, and every public surface
  /// that deposits an event must satisfy that invariant.
  ///
  /// Routing differs by event kind:
  /// - `Event::DialRequested` is routed DIRECTLY into the private
  ///   `dial_pending` deque with `attempted = false`, bypassing the
  ///   inner Endpoint queue entirely. Otherwise a caller that calls
  ///   [`Self::poll_timeout`] WITHOUT an intervening
  ///   [`Self::poll_event`] sieve would see an empty `dial_pending`
  ///   (the requeued `DialRequested` is sitting in the inner queue
  ///   awaiting sieve) — the immediate-due rescue would skip,
  ///   `poll_timeout` would degrade to the next gossip / probe /
  ///   quinn timer or the inner endpoint's own term, and the dial
  ///   would only be examined at the entry's `deadline` ≈
  ///   `now + stream_timeout` (silent strand at the deadline).
  ///   Direct routing ensures the entry IS present the moment
  ///   `requeue_event` returns.
  /// - Every other variant delegates to [`Endpoint::requeue_event`]
  ///   for observation via the next [`Self::poll_event`] — the standard
  ///   forwarded-event reordering pattern a harness uses to put an event
  ///   back at the tail of the queue after peeking.
  pub fn requeue_event(&mut self, ev: Event<I, SocketAddr>, now: Instant) {
    self.last_now = Some(now);
    match ev {
      Event::DialRequested(dial) => {
        let (id, peer, deadline) = dial.into_parts();
        self.dial_pending.push_back(PendingDial {
          id,
          peer,
          deadline,
          attempted: false,
        });
        // A freshly-deposited intent is unattempted; bump the incremental count
        // `refresh_immediate_due` reads for its immediate-due wake.
        self.unattempted_dial_count += 1;
        // Register the intent's own deadline; the unattempted immediate-due
        // half is folded live by `refresh_immediate_due`.
        self.deadline_index.set(TimerKey::Dial(id), Some(deadline));
      }
      other => self.ep.requeue_event(other),
    }
  }

  /// Next membership/lifecycle event for the driver, if any.
  ///
  /// `Event::DialRequested` is sieved out of the inner endpoint's queue
  /// into the private [`dial_pending`](Self::dial_pending) deque and is
  /// NEVER returned to external callers: in the composed design the
  /// coordinator IS the driver and dials itself (see [`Self::service_dials`])
  /// — leaking the retry token would let an external `poll_event` drain
  /// mid-handshake silently drop it, orphaning the pending stream intent
  /// (the push/pull or reliable-ping would never open). External callers
  /// only observe application-visible events.
  pub fn poll_event(&mut self) -> Option<Event<I, SocketAddr>> {
    loop {
      match self.ep.poll_event()? {
        Event::DialRequested(dial) => {
          let (id, peer, deadline) = dial.into_parts();
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
          // A freshly-sieved intent is unattempted; bump the incremental count
          // `refresh_immediate_due` reads for its immediate-due wake.
          self.unattempted_dial_count += 1;
          // Register the intent's own deadline; the unattempted immediate-due
          // half is folded live by `refresh_immediate_due`.
          self.deadline_index.set(TimerKey::Dial(id), Some(deadline));
          continue;
        }
        other => return Some(other),
      }
    }
  }

  /// Unified next-deadline = `min` over the memberlist endpoint, every
  /// bridge stream, every quinn connection, AND every pending-dial intent's
  /// own deadline. Returns an immediate-due wake (an `Instant` already
  /// `<= caller's now`) whenever `dial_pending` holds an entry
  /// `service_dials` has not yet attempted, or any connection carries a
  /// deferred `ConnectionEvent` backlog.
  ///
  /// **Amortized O(1).** Production drivers re-poll this after every inbound
  /// receive batch, so it must not fold an O(connections + bridges + dials)
  /// minimum per call — corrupted or replayed traffic would otherwise force
  /// that scan per batch. The minimum is kept incrementally in
  /// [`Self::deadline_index`]: every deadline-mutating site registers the
  /// affected [`TimerKey`], so this method reads the live minimum straight from
  /// the heap without touching the connection or bridge tables. The two cheap
  /// terms — the membership endpoint's own O(1) `poll_timeout` and the
  /// immediate-due anchor (derived from the few pending dials plus the O(1)
  /// pending-events flag) — are refreshed here at the single read point rather
  /// than chased through every membership / dial / `last_now` mutator; the
  /// O(connections + bridges) terms and each per-dial deadline are the ones
  /// maintained at their mutation sites. The returned instant is identical to
  /// the old O(N) fold, cross-checked in tests against
  /// [`Self::recompute_earliest_bruteforce`] after every operation.
  ///
  /// The pending-dial deadline term is correctness, not optimisation: when
  /// a dial intent is requeued onto `dial_pending` (the connection is still
  /// handshaking, or the established connection's MAX_STREAMS credit is
  /// exhausted), the next service tick must happen no later than that
  /// intent's own `deadline` — otherwise a fully-stalled `dial_pending`
  /// queue would be starved of wake-ups and the intent would only be
  /// re-examined when some unrelated component (the memberlist endpoint,
  /// an active stream, or a quinn connection) happens to wake the
  /// coordinator. On a quiet cluster that wake could be arbitrarily far
  /// after the intent's `deadline`, postponing the `dial_failed` past the
  /// user-visible exchange timeout.
  ///
  /// The immediate-due term is defence-in-depth for callers that bypass
  /// the high-level [`Self::start_push_pull`] / [`Self::start_reliable_ping`]
  /// / [`Self::start_user_message`] wrappers (which dial in-band) and queue
  /// a `DialRequested` directly via `endpoint_mut()`. A caller that drains
  /// [`Self::poll_event`] (sieving the `DialRequested` into `dial_pending`)
  /// and then advances solely by `poll_timeout` would otherwise only see
  /// the intent's own `deadline` ≈ `now + stream_timeout` — by the time
  /// that wake fires, `service_dials` discovers the deadline elapsed and
  /// retires the intent via `dial_failed`, never attempting the handshake.
  /// Returning `Some(last_now)` for any unattempted entry forces the next
  /// `handle_timeout(now)` to fire promptly, so `service_dials` attempts
  /// the dial in real wall-clock time of the same logical instant. Once an
  /// entry is attempted (whether the bidi opened or it was requeued for
  /// handshake / credit), subsequent wake-ups are driven by the
  /// connection's own `poll_timeout` and by `deadline`; re-firing an
  /// attempted entry every tick would busy-loop a still-handshaking
  /// connection. The deferred-`ConnectionEvent` half of the immediate-due
  /// term surfaces the one-tick-deferred CID / reset-token feedback
  /// `service_quinn` queues (mirroring quinn-proto's reference async driver),
  /// so a strict-poll driver re-enters `service_quinn` to drain it rather than
  /// sleeping until an unrelated idle / loss / probe timer fires.
  ///
  /// `last_now` is `None` only before the very first `handle_*` /
  /// `start_*` call: in that window the immediate-due wake degrades to the
  /// intent's `deadline` term, which is still strictly better than no
  /// wake at all (the dial fails at the deadline rather than orphaning).
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    // Refresh the two cheap terms at the single read point (robust against
    // every membership / dial / `last_now` mutator), then read the live
    // minimum from the incrementally-maintained index.
    self
      .deadline_index
      .set(TimerKey::Endpoint, self.ep.poll_timeout());
    self.refresh_immediate_due();
    self.deadline_index.earliest()
  }

  /// Recompute the immediate-due anchor key from small state: `Some(last_now)`
  /// when a freshly-sieved dial is still unattempted OR any connection holds a
  /// deferred `ConnectionEvent` backlog, else absent. Reads the O(1)
  /// `unattempted_dial_count` and `conns_with_pending_events` flags — never
  /// scans `dial_pending` or the connection table — so it stays cheap on the
  /// `poll_timeout` hot path a driver re-polls per inbound receive.
  fn refresh_immediate_due(&mut self) {
    let has_unattempted = self.unattempted_dial_count > 0;
    let has_pending_conn_events = !self.conns_with_pending_events.is_empty();
    let anchor = if has_unattempted || has_pending_conn_events {
      // `last_now` is `None` only before the first `handle_*` / `start_*`; the
      // `.flatten()` degrades that corner to "no anchor added", matching the
      // old fold's `if let Some(anchor) = self.last_now` guard.
      self.last_now
    } else {
      None
    };
    self.deadline_index.set(TimerKey::ImmediateDue, anchor);
  }

  /// Register connection `ch`'s current transport deadline in the index (or
  /// clear the key when the connection is gone). Called at connection-mutating
  /// sites that are NOT followed by a servicing tick's `collect_transmits`
  /// (which is the per-tick chokepoint that refreshes every surviving
  /// connection).
  fn index_conn(&mut self, ch: ConnectionHandle) {
    let deadline = self
      .conns
      .get_mut(ch)
      .and_then(|e| e.conn_mut().poll_timeout())
      .map(crate::Instant::from_std);
    self.deadline_index.set(TimerKey::Conn(ch), deadline);
  }

  /// Drop the reaped bridge's non-bucket index entries: its deadline-index
  /// [`TimerKey::Bridge`] key, its `(ch, sid)` reverse lookup in
  /// [`Self::bridge_by_conn_sid`], and — when it was an inbound (accepted)
  /// bridge — one unit of [`Self::inbound_bridge_count`]. Does NOT touch
  /// [`Self::bridges_by_conn`].
  ///
  /// The connection-loss reap uses this directly: it takes the whole
  /// `bridges_by_conn` bucket in one map remove and then runs this per bridge,
  /// so losing a connection holding K bridges costs O(K) with no per-bridge
  /// SmallVec surgery. [`Self::deindex_reaped_bridge`] wraps this and
  /// additionally `swap_remove`s the one bucket slot for a single-bridge reap.
  ///
  /// MUST run BEFORE [`Self::emit_exchange_completed`] for the same `id`.
  /// Inbound vs outbound is read off [`Self::pending_outbound_kinds`], which
  /// `emit_exchange_completed` drains for an outbound bridge; an inbound
  /// (accepted) bridge never enters that map, so `!contains_key` is the inbound
  /// predicate — the same definition the mint-time increment uses.
  fn deindex_reaped_bridge_bucketless(&mut self, id: StreamId, ch: ConnectionHandle, sid: QuicSid) {
    self.deadline_index.set(TimerKey::Bridge(id), None);
    self.bridge_by_conn_sid.remove(&(ch, sid));
    if !self.pending_outbound_kinds.contains_key(&id) {
      debug_assert!(
        self.inbound_bridge_count > 0,
        "inbound_bridge_count underflow reaping inbound bridge {id:?}"
      );
      self.inbound_bridge_count = self.inbound_bridge_count.saturating_sub(1);
    }
  }

  /// Drop every incremental index entry a single reaped bridge held — the
  /// bucketless entries via [`Self::deindex_reaped_bridge_bucketless`], plus its
  /// slot in the per-connection [`Self::bridges_by_conn`] bucket via one O(1)
  /// [`Self::index_bridge_reap`]. `conn_slot` is the bridge's recorded bucket
  /// position, which the caller MUST capture (alongside `ch`/`sid`) from the
  /// bridge BEFORE dropping it. MUST run BEFORE [`Self::emit_exchange_completed`]
  /// for the same `id`.
  fn deindex_reaped_bridge(
    &mut self,
    id: StreamId,
    ch: ConnectionHandle,
    sid: QuicSid,
    conn_slot: usize,
  ) {
    #[cfg(test)]
    {
      self.counters.bridge_pump_path_reaps = self.counters.bridge_pump_path_reaps.saturating_add(1);
    }
    self.deindex_reaped_bridge_bucketless(id, ch, sid);
    self.index_bridge_reap(id, ch, conn_slot);
  }

  /// Drop machine stream `id` from connection `ch`'s [`Self::bridges_by_conn`]
  /// bucket in O(1) via `swap_remove(conn_slot)`, then repair the back-pointer of
  /// whichever sibling `swap_remove` relocated into the vacated slot. Removes the
  /// bucket entry once it empties, so the map holds a key only while `ch` owns at
  /// least one bridge.
  ///
  /// `conn_slot` is the bridge's recorded [`Bridge::conn_slot`]; the invariant
  /// `bridges_by_conn[ch][conn_slot] == id` is asserted in debug. `swap_remove`
  /// touches at most one element (the relocated tail), so a single reap is O(1)
  /// and a K-bridge burst is O(K) — where the former `retain` scanned the whole
  /// bucket per reap (O(K) each, O(K²) per burst).
  ///
  /// A method, not a free function like [`index_bridge_mint`]: its sole caller
  /// [`Self::deindex_reaped_bridge`] holds full `&mut self` (a reap never runs
  /// under a live `self.conns` borrow), and it needs `self.bridges` for the
  /// sibling fixup. The relocated sibling is always present in `self.bridges`:
  /// the reaped bridge was already removed from `self.bridges` by the caller (so
  /// it is never the one relocated), and every bucket id has a live `bridges`
  /// entry by the index invariant.
  fn index_bridge_reap(&mut self, id: StreamId, ch: ConnectionHandle, conn_slot: usize) {
    // Scope the bucket borrow so the sibling fixup and the empty-bucket removal
    // can re-borrow `self.bridges` / `self.bridges_by_conn` afterwards.
    let (relocated_sibling, emptied) = match self.bridges_by_conn.get_mut(&ch) {
      Some(bucket) => {
        let removed = bucket.swap_remove(conn_slot);
        debug_assert_eq!(
          removed, id,
          "conn_slot back-pointer must index this bridge's own StreamId"
        );
        // `swap_remove` moved the bucket's last element into `conn_slot` — unless
        // `conn_slot` WAS the last slot, in which case nothing moved and
        // `get(conn_slot)` is now None (a bare `bucket[conn_slot]` would panic).
        (bucket.get(conn_slot).copied(), bucket.is_empty())
      }
      None => return,
    };
    #[cfg(test)]
    {
      // One O(1) swap_remove touched a single element. The reverted `retain`
      // scans the whole bucket per reap, so counting each scanned element makes
      // this climb to O(K²) across a K-bridge burst — the negative control the
      // visit-count tests assert against.
      self.counters.bridge_bucket_scan_ops = self.counters.bridge_bucket_scan_ops.saturating_add(1);
    }
    if let Some(sibling_id) = relocated_sibling {
      if let Some(br) = self.bridges.get_mut(&sibling_id) {
        br.set_conn_slot(conn_slot);
      }
    }
    if emptied {
      self.bridges_by_conn.remove(&ch);
    }
  }

  /// Brute-force earliest deadline — a byte-for-byte fold of the same sources
  /// the incremental [`Self::deadline_index`] tracks, kept as the invariant
  /// oracle. A test asserts [`Self::poll_timeout`] equals this after every
  /// public operation, so a missed `set` at any deadline-mutating site is
  /// caught as a divergence rather than shipped as a silent stale index.
  #[cfg(test)]
  fn recompute_earliest_bruteforce(&mut self) -> Option<Instant> {
    let mut best = self.ep.poll_timeout();
    for b in self.bridges.values() {
      if let Some(t) = b.poll_timeout() {
        best = Some(best.map_or(t, |b| b.min(t)));
      }
    }
    let mut has_pending_conn_events = false;
    for ch in self.conns.iter_handles() {
      if let Some(e) = self.conns.get_mut(ch) {
        if let Some(t) = e.conn_mut().poll_timeout().map(crate::Instant::from_std) {
          best = Some(best.map_or(t, |b| b.min(t)));
        }
        if e.has_pending_events() {
          has_pending_conn_events = true;
        }
      }
    }
    let mut has_unattempted = false;
    for entry in &self.dial_pending {
      let t = entry.deadline;
      best = Some(best.map_or(t, |b| b.min(t)));
      if !entry.attempted {
        has_unattempted = true;
      }
    }
    // Parked (attempted) intents own the same `Dial` deadline key as fresh ones,
    // so fold their deadlines too; they never contribute an immediate-due wake
    // (they are all `attempted`).
    for bucket in self.dial_parked.values() {
      for entry in bucket {
        let t = entry.deadline;
        best = Some(best.map_or(t, |b| b.min(t)));
      }
    }
    if has_unattempted || has_pending_conn_events {
      if let Some(anchor) = self.last_now {
        best = Some(best.map_or(anchor, |b| b.min(anchor)));
      }
    }
    best
  }

  /// Test-only: the incremental live-inbound-bridge count.
  #[cfg(test)]
  fn inbound_bridge_count(&self) -> usize {
    self.inbound_bridge_count
  }

  /// Test-only brute-force recount of live INBOUND bridges — the bridges absent
  /// from `pending_outbound_kinds`, read straight from `self.bridges`. The
  /// invariant is `inbound_bridge_count == this recount` at all times; a missed
  /// increment/decrement makes them diverge, so the maintenance test asserts
  /// their equality after every mint / reap.
  #[cfg(test)]
  fn inbound_bridge_count_recount(&self) -> usize {
    self
      .bridges
      .keys()
      .filter(|id| !self.pending_outbound_kinds.contains_key(id))
      .count()
  }

  /// Test-only: the incremental unattempted-pending-dial count.
  #[cfg(test)]
  fn unattempted_dial_count(&self) -> usize {
    self.unattempted_dial_count
  }

  /// Test-only brute-force recount of unattempted pending dials, read straight
  /// from `dial_pending`. The invariant is `unattempted_dial_count == this
  /// recount` at all times; an off-by-one at any `dial_pending` mutation site
  /// silently drops or forces an immediate-due wake, so the maintenance test
  /// asserts their equality after every operation.
  ///
  /// Also asserts the structural invariant that every PARKED intent is
  /// `attempted` — the split's load-bearing property: a fresh (`attempted ==
  /// false`) intent must live only in the `dial_pending` FIFO (so it is counted
  /// here), never in a `dial_parked` bucket. A parked unattempted intent would
  /// silently drop from this count and never fire its immediate-due wake.
  #[cfg(test)]
  fn unattempted_dial_recount(&self) -> usize {
    assert!(
      self.dial_parked.values().flatten().all(|d| d.attempted),
      "every dial_parked entry must be attempted; a fresh intent belongs in the \
       dial_pending FIFO"
    );
    self.dial_pending.iter().filter(|d| !d.attempted).count()
  }

  /// Next typed unreliable memberlist [`Transmit`] for the driver to encode
  /// onto the unreliable (UDP) path, if any.
  ///
  /// Each call drains ONE `Transmit` straight out of the inner
  /// `Endpoint::poll_transmit`; nothing is prebuffered coordinator-internally.
  /// This makes the inner pop — which decrements `Endpoint`'s leave-completion
  /// counter and emits `Event::LeftCluster` after the last dead-self notice
  /// — happen at the SAME moment the datagram crosses to the external
  /// driver. A caller that `leave(now)`s, ticks, and then reads `poll_event`
  /// cannot observe `LeftCluster` until it has drained the dead-self tail
  /// through this accessor: tearing the socket down on `LeftCluster` therefore
  /// guarantees every dead-self broadcast has been handed to the driver, so
  /// peers see `Dead`/`Left` rather than wrongly Suspecting.
  ///
  /// The driver MUST take the unreliable path through this accessor and never
  /// call `endpoint_mut().poll_transmit()` directly (that would double-drive
  /// the `LeftCluster` boundary).
  pub fn poll_memberlist_transmit(&mut self) -> Option<Transmit<I, SocketAddr>> {
    self.ep.poll_transmit()
  }

  /// Refuse an inbound Initial that exceeded a connection-admission cap: count
  /// the rejection and drop it via `Endpoint::ignore`. `ignore` frees quinn's
  /// own incoming-buffer bookkeeping for this `Incoming` (merely dropping it
  /// would leak that) and sends nothing back, so a spoofed source gets no
  /// reflected bytes. No `ConnTable` entry is created.
  fn reject_incoming(&mut self, incoming: quinn_proto::Incoming) {
    self.ep.metrics_mut().quic_connections_rejected += 1;
    self.quinn.ignore(incoming);
  }

  /// Route one inbound `DatagramEvent` and return the [`ConnectionHandle`] the
  /// caller must service, or `None` when the datagram addresses no connection.
  ///
  /// Servicing is bounded to the single addressed connection (see
  /// [`Self::service_connection`]), so this never gates on inferred "progress":
  /// even a packet the connection will discard (failed AEAD authentication, a
  /// replayed packet number, a forbidden migration) is worth the O(that
  /// connection) servicing pass, and forcing that pass costs an attacker exactly
  /// the connection whose live CID they already know — never the whole table.
  ///
  /// - `ConnectionEvent` for a live handle: apply it, return `Some(ch)`.
  /// - `ConnectionEvent` for an unknown (already-reaped) handle: `None`.
  /// - `NewConnection` accepted into the table: `Some(ch)`; over-cap, rejected,
  ///   or a failed `accept`: `None` (any owed close bytes are queued to `out`).
  /// - stateless `Response`: `None` (the bytes are queued to `out`).
  fn route_datagram_event(
    &mut self,
    ev: DatagramEvent,
    from: SocketAddr,
    now: Instant,
    scratch: &[u8],
  ) -> Option<ConnectionHandle> {
    match ev {
      DatagramEvent::ConnectionEvent(ch, cev) => {
        // A `ConnectionEvent` for an unknown handle (already reaped) applies
        // nothing and addresses no live connection.
        match self.conns.get_mut(ch) {
          Some(e) => {
            // quinn routed this packet by its plaintext DCID before
            // authenticating it, so a live handle does not imply the packet will
            // be accepted — `Connection::handle_event` silently discards one that
            // fails AEAD auth, replays a seen packet number, or would migrate a
            // forbidden path. Servicing is scoped to this one connection, so we
            // never need to distinguish those from a genuine advance: apply the
            // event and return `ch`. A discarded packet then costs only an
            // O(this connection) pass; a genuine close / stateless-reset drives
            // the connection to drained and the same pass reaps it and its
            // bridges. `service_connection` refreshes this connection's deadline
            // key, so no inline `set` is needed here.
            e.conn_mut().handle_event(cev);
            Some(ch)
          }
          None => None,
        }
      }
      DatagramEvent::NewConnection(incoming) => {
        // Bound connection-table growth from unauthenticated inbound Initials
        // BEFORE any persistent per-connection state is committed. An Initial is
        // unauthenticated (the TLS handshake has not run), so a flood — from one
        // source with varied DCIDs, or many spoofed sources — would otherwise
        // grow the `ConnTable` slab without bound (memory exhaustion + O(N)
        // per-tick scans). mutual-TLS and Retry time-bound this state; these two
        // caps are the additional hard bound. The global cap limits total
        // tracked connections; the per-source cap limits one address's
        // concurrent half-open handshakes so no single source can consume the
        // global budget. Past either cap the Initial is dropped (see
        // `reject_incoming`) and no `ConnTable` entry is created.
        //
        // Global cap FIRST, short-circuiting the per-source check. At
        // connection-table saturation an attacker floods fresh-DCID Initials;
        // each must be refused with no per-datagram work that scales with the
        // table. The global check is an O(1) slab-length compare, so a saturated
        // endpoint rejects here and never reaches the per-source lookup below.
        if self
          .cfg
          .max_quic_connections()
          .is_some_and(|max| self.conns.len() >= max)
        {
          self.reject_incoming(incoming);
          // The Initial was dropped and no connection-table state was committed,
          // so this datagram addresses no connection.
          return None;
        }
        // Per-source pending cap. Reached only under the global cap.
        // `pending_inbound_from` is an O(1) index read (not a scan of the
        // connection slab), so this too is attacker-flood-safe.
        if let Some(max) = self.cfg.max_pending_connections_per_source() {
          #[cfg(test)]
          {
            self.counters.quic_pending_inbound_checks =
              self.counters.quic_pending_inbound_checks.saturating_add(1);
          }
          if self.conns.pending_inbound_from(&from) >= max {
            self.reject_incoming(incoming);
            return None;
          }
        }
        let mut buf = Vec::new();
        match self.quinn.accept(
          incoming,
          now.into_std(),
          &mut buf,
          Some(self.cfg.server_arc()),
        ) {
          Ok((ch, conn)) => {
            self.conns.insert_accepted(ch, conn, from);
            // Register the freshly-accepted connection's deadline the moment it
            // enters the table; the servicing pass this accept triggers refreshes
            // it again via `collect_conn_transmits`.
            self.index_conn(ch);
            // A new connection was committed to the table — service it.
            Some(ch)
          }
          Err(e) => {
            // quinn-proto attaches an `Option<Transmit>` to its `AcceptError`
            // whenever `accept` owes a refusal/close to the peer (CID
            // exhaustion + initial-handshake transport failure produce an
            // `initial_close` response). The close bytes are already in our
            // local `buf`; surface them via the driver-facing `out` queue,
            // mirroring the `DatagramEvent::Response` arm below. Without
            // this the peer waits its full handshake retransmit budget
            // instead of seeing the immediate close.
            if let Some(t) = e.response {
              if t.size <= buf.len() {
                self
                  .out
                  .push_back((t.destination, Bytes::copy_from_slice(&buf[..t.size])));
                #[cfg(test)]
                {
                  self.counters.accept_error_responses_emitted = self
                    .counters
                    .accept_error_responses_emitted
                    .saturating_add(1);
                }
              }
            }
            // The accept failed: only a close response (if any) was queued to
            // `out`; no connection was created.
            None
          }
        }
      }
      DatagramEvent::Response(t) => {
        // `Endpoint::handle` wrote `t.size` bytes of a stateless response
        // (Retry / version negotiation / stateless reset) into the `scratch`
        // buffer passed in `handle_udp`; surface it as an outbound datagram.
        if t.size <= scratch.len() {
          self
            .out
            .push_back((t.destination, Bytes::copy_from_slice(&scratch[..t.size])));
        }
        // A stateless response addresses no local connection.
        None
      }
    }
  }

  fn handle_memberlist_udp(&mut self, from: SocketAddr, datagram: &[u8]) {
    // The umbrella `codec` is not a dependency of memberlist-proto, so the
    // byte-level decode (decode_incoming -> parse_messages -> handle_packet)
    // cannot run in-crate. Surfacing the raw datagram as an explicit ingress
    // action — never a silent no-op — is required for the composed unit's
    // ingress to remain correct: a no-op here would lose every UDP
    // ping/ack/alive/suspect on the composed unit's public ingress. The
    // codec-owning layer drains it via `poll_memberlist_ingress`, decodes
    // each `Message`, and feeds it back through `handle_packet`.
    //
    // Admission goes through the SAME capped helper as the QUIC datagram drain
    // so the shared coordinator queue is bounded uniformly: a plain-UDP /
    // fallback flood cannot bypass the per-peer or node-global cap, drive
    // `mem_ingress_per_peer` past the bound the QUIC drain checks, or push the
    // global count over the hard memory limit.
    push_mem_ingress_capped(
      &mut self.mem_ingress,
      &mut self.mem_ingress_per_peer,
      &mut self.datagram_ingress_dropped,
      from,
      || Bytes::copy_from_slice(datagram),
    );
  }

  /// Emit [`Event::ExchangeCompleted`] for an outbound bridge that has
  /// reached its terminal state. `id` MUST be the bridge's
  /// machine-level [`StreamId`] (the key the bridge was inserted into
  /// `self.bridges` under). The helper drains the originating kind from
  /// [`Self::pending_outbound_kinds`] (`None` ⇒ inbound or unknown —
  /// no emission) and the peer address from
  /// [`Self::pending_outbound_peers`].
  ///
  /// Called from every bridge-reap site (the `pump_bridges` D1 reap,
  /// the `service_quinn` ConnectionLost / `is_drained()` inline drain,
  /// and the test-only acceptance-tracking pump). Outbound only —
  /// inbound (server-accepted) bridges have no entry in
  /// `pending_outbound_kinds` and silently no-op here.
  fn emit_exchange_completed(&mut self, id: StreamId, outcome: ExchangeStatus) {
    let Some(kind) = self.pending_outbound_kinds.remove(&id) else {
      return;
    };
    // `pending_outbound_peers` is always populated alongside
    // `pending_outbound_kinds`; if `kind` was present, peer must be too.
    let peer = self
      .pending_outbound_peers
      .remove(&id)
      .expect("pending_outbound_peers entry must exist when kind entry exists");
    self
      .ep
      .emit_event(Event::ExchangeCompleted(ExchangeCompleted::new(
        ExchangeId::from(id),
        peer,
        outcome,
        kind,
      )));
  }

  /// Retire dial intent `id` after the in-band dial failed BEFORE any
  /// bridge was created (deadline elapsed, cached connection closed,
  /// `get_or_dial` error, or a frozen-API `dial_succeeded == None`).
  ///
  /// Discards the staged kind + peer (the bridge that would have been
  /// keyed by `id` never existed, so the `emit_exchange_completed`
  /// reap path will never observe `id`), then routes the failure
  /// through the inner FSM's `dial_failed`.
  ///
  /// For an `ExchangeKind::UserMessage` OR `ExchangeKind::PushPull` dial
  /// it ALSO emits a terminal [`Event::ExchangeCompleted`] with
  /// `outcome = Failed`, keyed by the SAME `ExchangeId::from(id)` the
  /// QUIC driver parked its waiter under (the reliable-send waiter for
  /// `UserMessage`, the `WaitForCompletion` join waiter for `PushPull`).
  /// Without this the driver's parked waiter would hang forever: the only
  /// other `ExchangeCompleted` producer is the bridge-reap path, which
  /// never fires for a bridge that was never created. A QUIC join parks
  /// every `start_push_pull` exchange keyed by `ExchangeId::from(StreamId)`
  /// and resolves only when each parked id surfaces a terminal
  /// `ExchangeCompleted(PushPull)`; an unreachable seed whose dial fails
  /// before a bridge exists would otherwise never drain its waiter set.
  ///
  /// No double-completion: a pre-bridge failure means NO bridge keyed by
  /// `id` was ever inserted into `self.bridges` (the bridge is created
  /// only on the `dial_succeeded` success path, which does not call this),
  /// and this method drains both staged maps up front, so the bridge-reap
  /// `emit_exchange_completed` — which requires both a live bridge for `id`
  /// AND a `pending_outbound_kinds` entry — can never also fire for this
  /// `id`. This single `Failed` is the lone terminal event for the
  /// StreamId, independent of kind.
  ///
  /// `ReliablePing` is NOT widened here: its failure is already driven
  /// into the probe FSM by `dial_failed` itself, which terminalizes the
  /// fallback-probe path; a second `ExchangeCompleted` is neither parked
  /// on nor expected by any driver for the reliable-ping fallback.
  fn retire_failed_dial(&mut self, id: StreamId, err: StreamError, now: Instant) {
    let kind = self.pending_outbound_kinds.remove(&id);
    let peer = self.pending_outbound_peers.remove(&id);
    if let (Some(kind @ (ExchangeKind::UserMessage | ExchangeKind::PushPull)), Some(peer)) =
      (kind, peer)
    {
      self
        .ep
        .emit_event(Event::ExchangeCompleted(ExchangeCompleted::new(
          ExchangeId::from(id),
          peer,
          ExchangeStatus::Failed,
          kind,
        )));
    }
    self.ep.dial_failed(id, err, now);
  }

  /// Determine the [`ExchangeStatus`] of a bridge at the moment it is
  /// being reaped. Mirrors [`super::streams::StreamEndpoint::reap_bridge`]'s
  /// rule: any failure phase (`BridgeFailure::Timeout`, `Transport`,
  /// `Decode`, `ConnectionLost`, `AdmissionClosed`, `DialRetired`,
  /// `EncryptionPolicyChanged`) maps to `Failed`; the cooperative
  /// `BothClosed` clean terminus maps to `Succeeded`. The bridge MUST
  /// be terminal before this is called — terminal-after-D1 is the only
  /// site that knows the final outcome.
  #[inline]
  fn outcome_for_terminal(br: &Bridge<I, SocketAddr>) -> ExchangeStatus {
    if br.is_phase_failed() {
      ExchangeStatus::Failed
    } else {
      ExchangeStatus::Succeeded
    }
  }

  /// Shared tail of [`Self::run_tick`] and [`Self::flush_outbound`]:
  /// step (5) connection drained-reap, then [`Self::collect_transmits`].
  ///
  /// The reap simply walks every live `ConnectionHandle` and calls
  /// [`ConnTable::reap_if_drained`]; per-connection deferred
  /// `ConnectionEvent`s queued by `service_quinn` live in each
  /// [`super::conn::ConnEntry`]'s own `pending_events` deque (see
  /// [`super::conn::ConnEntry::queue_pending_event`]) so a reap that drops
  /// the slab entry also drops its deferred queue by construction — no
  /// global FIFO can survive past the reap to be re-keyed onto a fresh
  /// connection occupying the freed slab slot.
  fn finalize_tick(&mut self, now: Instant) {
    for ch in self.conns.iter_handles() {
      if self.conns.reap_if_drained(&mut self.quinn, ch) {
        // The connection left the table: drop its deadline key and any
        // pending-events membership so neither lingers as a stale index term.
        self.deadline_index.set(TimerKey::Conn(ch), None);
        self.conns_with_pending_events.remove(&ch);
      }
    }
    self.collect_transmits(now);
    // The global tick services every bridge directly via `pump_bridges` (which
    // clears every `queued` flag at pump entry), so any ready-queue entries a
    // mint trigger pushed this tick are now flag-dead; clear the pass-scoped
    // queue so it never carries a stale id into a later pass.
    self.ready_bridges.clear();
    self.debug_assert_ready_drained();
  }

  /// Debug-only pass-end invariant: the pass-scoped [`Self::ready_bridges`] queue
  /// is empty and no live bridge still carries a set [`Bridge::queued`] flag.
  ///
  /// Queue/flag lockstep: a trigger sets the flag only when it pushes the id, and
  /// the flag is cleared only at pump entry — either by a queue pop-drain
  /// ([`Self::drain_ready_bridges`]) or by the global pump-all
  /// ([`Self::pump_bridges`], which services every bridge and so clears every
  /// flag) — or with the bridge on reap. The queue is emptied only by a
  /// drain-to-empty or [`Self::finalize_tick`]'s clear. Both invariants therefore
  /// hold at the end of every servicing pass; the assert guards a future trigger
  /// site that enqueues without a matching drain. Compiled out in release.
  fn debug_assert_ready_drained(&self) {
    debug_assert!(
      self.ready_bridges.is_empty() && self.bridges.values().all(|br| !br.queued()),
      "ready-bridge queue not fully drained at pass end: {} queued entries, any-flag-set {}",
      self.ready_bridges.len(),
      self.bridges.values().any(|br| br.queued()),
    );
  }

  /// Move any `Event::DialRequested` currently in the inner endpoint's
  /// queue into the private [`dial_pending`](Self::dial_pending) deque,
  /// preserving FIFO order of every other event. The inner queue is
  /// fully drained into a local buffer; `DialRequested` is routed to
  /// `dial_pending`; every other event is re-queued at the back via
  /// [`Endpoint::requeue_event`] in original order. Bounded — each event
  /// is visited at most once because the drain stops when the inner
  /// queue is empty and re-queueing into the now-empty queue cannot
  /// re-surface anything we have already taken out.
  fn sieve_dial_events(&mut self) {
    let mut others: Vec<Event<I, SocketAddr>> = Vec::new();
    while let Some(ev) = self.ep.poll_event() {
      match ev {
        Event::DialRequested(dial) => {
          let (id, peer, deadline) = dial.into_parts();
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
          // Keep the unattempted count exact even mid-sieve; `service_dials`
          // resets it to 0 at its `mem::take` immediately after this call.
          self.unattempted_dial_count += 1;
        }
        other => others.push(other),
      }
    }
    for ev in others {
      self.ep.requeue_event(ev);
    }
  }

  fn collect_transmits(&mut self, now: Instant) {
    // Memberlist unreliable Transmit is NOT pre-drained here. Each call to
    // `poll_memberlist_transmit` drains one `Transmit` out of
    // `Endpoint::poll_transmit` on demand, so the inner pop — which counts
    // down the leave-completion boundary and emits `Event::LeftCluster` after
    // the last dead-self notice — happens exactly when the datagram crosses
    // to the external driver. Pre-draining coordinator-internally would tick
    // the boundary on the inner-queue→buffer hop and let a caller observe
    // `LeftCluster` while the dead-self bytes still sat in the buffer,
    // leaving peers to wrongly Suspect after teardown.
    //
    // quinn datagrams (handshake, stream data, ACKs, close) HAVE no such
    // dead-self accounting on their inner pop, so pre-draining them into
    // `out` is fine and keeps `poll_transmit` a constant-time `pop_front`.
    for ch in self.conns.iter_handles() {
      self.collect_conn_transmits(ch, now);
    }
  }

  /// Drain one connection's owed outbound quinn datagrams into `out` and refresh
  /// its deadline-index key. The per-connection body of [`Self::collect_transmits`]
  /// — the global tick loops it over every handle; the per-datagram
  /// [`Self::service_connection`] path calls it for the single serviced
  /// connection so exactly that connection's transmits flush without an
  /// O(all connections) pass.
  fn collect_conn_transmits(&mut self, ch: ConnectionHandle, now: Instant) {
    let Some(e) = self.conns.get_mut(ch) else {
      return;
    };
    let mut buf = Vec::new();
    while let Some(tr) = e.conn_mut().poll_transmit(now.into_std(), 1, &mut buf) {
      // Use the transmit's own destination (not the cached peer) so a
      // datagram is sent to the address quinn selected — correct under
      // path migration and consistent with the stateless `Response` arm.
      self
        .out
        .push_back((tr.destination, Bytes::copy_from_slice(&buf[..tr.size])));
      buf.clear();
    }
    // Refresh this connection's deadline term now that its transmit queue is
    // drained — the last touch of the connection in whichever pass called this,
    // so the live `Conn` key is current without a separate `set` at the earlier
    // `service_one_conn` / `service_dials` mutation sites.
    let deadline = e.conn_mut().poll_timeout().map(crate::Instant::from_std);
    self.deadline_index.set(TimerKey::Conn(ch), deadline);
  }

  /// Which wire the unreliable path (gossip + probes) rides. Delegates to the
  /// [`QuicOptions`]; the driver reads it to route each unreliable send onto
  /// either [`queue_unreliable_datagram`](Self::queue_unreliable_datagram) or
  /// the plain-UDP fallback.
  pub fn unreliable_transport(&self) -> UnreliableTransport {
    self.cfg.unreliable_transport()
  }

  /// Count of unreliable datagrams dropped on a residual quinn datagram-state
  /// error (best-effort accounting; never a membership signal).
  #[cfg(test)]
  pub(crate) fn datagram_dropped(&self) -> u64 {
    self.datagram_dropped
  }

  /// Count of inbound unreliable datagrams dropped by the receive drain because
  /// `mem_ingress` was at the count cap (best-effort accounting; never a
  /// membership signal).
  #[cfg(test)]
  pub(crate) fn datagram_ingress_dropped(&self) -> u64 {
    self.datagram_ingress_dropped
  }

  /// Count of membership-time advances ([`Endpoint::handle_timeout`] calls).
  /// A QUIC packet ingress must NOT bump this — only the driver's explicit
  /// `handle_timeout` may. See [`Self::service_connection`].
  #[cfg(test)]
  pub(crate) fn membership_time_advances(&self) -> u64 {
    self.counters.membership_time_advances
  }

  /// The datagram `max_size` of the pooled connection to `peer`, or `None` if
  /// no connection exists or datagrams are not yet negotiated.
  #[cfg(test)]
  pub(crate) fn connection_datagram_max_size(&mut self, peer: SocketAddr) -> Option<usize> {
    let ch = self.conns.handle_for(&peer)?;
    self.conns.get_mut(ch)?.conn_mut().datagrams().max_size()
  }

  /// Offer one already-encoded unreliable datagram (gossip or probe) to `peer`
  /// over its pooled QUIC connection. Routes only the WIRE — the bytes are the
  /// same the plain-UDP path would send. Connection liveness is never a
  /// membership signal: a `NotReady`/dropped datagram becomes a probe timeout,
  /// not a `Suspect`. The driver falls back to plain UDP on a non-`Queued`
  /// outcome so dissemination is not starved.
  pub fn queue_unreliable_datagram(
    &mut self,
    peer: SocketAddr,
    bytes: Bytes,
    now: Instant,
  ) -> DatagramSendStatus {
    let sni_arc = self.cfg.sni_for(&peer);
    let ch = match self.conns.get_or_dial(
      &mut self.quinn,
      now,
      self.cfg.client().clone(),
      peer,
      &sni_arc,
      self.cfg.max_quic_connections(),
    ) {
      Ok(ch) => ch,
      // The datagram-fallback dial hit the global connection cap: this new
      // peer gets no QUIC connection. Count it against the same connection-cap
      // metric the inbound path uses and drop the datagram (best-effort; the
      // driver falls back to plain UDP so gossip is not starved).
      Err(conn::DialError::AtGlobalCap) => {
        self.ep.metrics_mut().quic_connections_rejected += 1;
        return DatagramSendStatus::NotReady;
      }
      // A dial that cannot even be initiated (ConnectError) — best effort.
      Err(conn::DialError::Connect(_)) => return DatagramSendStatus::NotReady,
    };
    // Compute the outcome, then unconditionally refresh the connection's
    // deadline key below: `get_or_dial` may have created a fresh connection and
    // `datagrams().send` rearms its transmit timer, and no servicing tick
    // follows this call to refresh it via `collect_transmits`.
    let status = match self.conns.get_mut(ch) {
      None => DatagramSendStatus::NotReady,
      Some(e) => {
        let conn = e.conn_mut();
        match conn.datagrams().max_size() {
          // Still handshaking, peer doesn't support datagrams, or disabled
          // locally. Best-effort: the driver falls back to UDP; a later
          // datagram lands once the connection is Established.
          None => DatagramSendStatus::NotReady,
          Some(max) if bytes.len() > max => DatagramSendStatus::TooLarge,
          // drop = false: under send-buffer pressure quinn returns Blocked and
          // leaves the already-queued datagrams intact, rather than evicting the
          // OLDEST to make room. The unreliable path carries probe Pings and
          // Acks as well as gossip, so evicting the oldest could silently drop
          // an in-flight probe and cause a spurious failure-detector timeout;
          // refusing the NEW datagram (the driver then falls back to plain UDP)
          // preserves the earlier critical datagrams and loses nothing.
          Some(_) => match conn.datagrams().send(bytes, false) {
            Ok(()) => DatagramSendStatus::Queued,
            // Send buffer full: the driver falls back to plain UDP for this
            // payload. Not a drop (the payload still goes out over UDP) and not
            // counted.
            Err(quinn_proto::SendDatagramError::Blocked(_)) => DatagramSendStatus::NotReady,
            // UnsupportedByPeer / Disabled / TooLarge are excluded by the
            // max_size pre-check above; any residual error is unexpected —
            // count and fall back.
            Err(_) => {
              self.datagram_dropped = self.datagram_dropped.saturating_add(1);
              DatagramSendStatus::NotReady
            }
          },
        }
      }
    };
    self.index_conn(ch);
    status
  }
}

// Membership-machine forwarders and wire encoders that need full node identity
// but never draw the gossip RNG.
impl<I, R> QuicEndpoint<I, R>
where
  I: crate::Id,
{
  /// Update the local node's metadata. The new value is gossiped
  /// through the standard alive-broadcast path.
  ///
  /// Pass-through to [`Endpoint::update_meta`]; the inner endpoint bumps
  /// the local incarnation and queues an `Alive` broadcast carrying the
  /// new bytes so peers converge to the updated metadata via the normal
  /// SWIM path.
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::NotRunning`] if the local lifecycle has
  /// already transitioned to `Leaving` / `Left` / `Shutdown`. Returns
  /// [`crate::error::Error::MetaExceedsCap`] if `meta` exceeds the
  /// per-endpoint cap configured at construction.
  pub fn update_meta(&mut self, meta: crate::typed::Meta) -> Result<(), Error> {
    self.ep.update_meta(meta)
  }

  /// Queue an application user-broadcast for gossip dissemination. Forwards
  /// to the inner membership [`Endpoint`].
  #[inline]
  pub fn queue_user_broadcast(&mut self, data: Bytes) -> Result<(), Error> {
    self.ep.queue_user_broadcast(data)
  }

  /// Queue an application user-broadcast at priority `rank` (`0` = highest)
  /// for gossip dissemination. Forwards to
  /// [`Endpoint::queue_user_broadcast_ranked`]; see that method for the
  /// strict-priority and rank-saturation contract.
  #[inline]
  pub fn queue_user_broadcast_ranked(&mut self, rank: u8, data: Bytes) -> Result<(), Error> {
    self.ep.queue_user_broadcast_ranked(rank, data)
  }

  /// Enqueue a directed unreliable user-data packet to `to`. Forwards to
  /// [`Endpoint::send_user_packet`]. Like `send_user_packets`, only touches
  /// the gossip `pending_transmits` queue, drained by
  /// `poll_memberlist_transmit`.
  ///
  /// Returns `Err` if the payload exceeds the configured `gossip_mtu` ceiling.
  #[inline]
  pub fn send_user_packet(&mut self, to: SocketAddr, data: Bytes) -> Result<(), Error> {
    self.ep.send_user_packet(to, data)
  }

  /// Set the application push-pull local-state snapshot. Forwards to the
  /// inner [`Endpoint`].
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::LocalStateExceedsFrame`] if the snapshot's
  /// framed PushPull would exceed the reliable-stream frame budget — such a
  /// snapshot is deterministically untransmittable, so it is rejected rather
  /// than stored.
  #[inline]
  pub fn set_local_state_snapshot(&mut self, bytes: Bytes) -> Result<(), Error> {
    self.ep.set_local_state_snapshot(bytes)
  }

  /// Set the application ack payload attached to probe acks. Forwards to the
  /// inner [`Endpoint`].
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::AckPayloadExceedsMtu`] if the framed Ack
  /// carrying `payload` would not fit the node's gossip packet budget — an
  /// over-budget Ack is deterministically unsendable on the gossip socket,
  /// so the payload is rejected rather than stored.
  #[inline]
  pub fn set_ack_payload(&mut self, payload: Bytes) -> Result<(), Error> {
    self.ep.set_ack_payload(payload)
  }

  /// Initiate one SWIM probe tick on the inner membership endpoint.
  ///
  /// Pass-through to [`Endpoint::start_probe`]. Sets `last_now` so the
  /// next `poll_timeout` is anchored to a known-past instant (the same
  /// idiom every other `handle_*` / `start_*` uses). The probe itself
  /// rides the unreliable UDP path (`poll_memberlist_transmit`); only if
  /// it fails does the reliable QUIC fallback kick in via the natural
  /// suspicion / failure-detection timing.
  pub fn start_probe(&mut self, now: Instant) -> bool {
    self.last_now = Some(now);
    self.ep.start_probe(now)
  }

  /// Inject a `Suspect` event on the inner membership endpoint
  /// (test-harness path; a real driver gets Suspect via SWIM probe
  /// timeouts or peer gossip).
  ///
  /// Pass-through to [`Endpoint::handle_suspect`]. Sets `last_now`.
  pub fn handle_suspect(
    &mut self,
    from: SocketAddr,
    suspect: crate::typed::Suspect<I>,
    at: Instant,
  ) {
    self.last_now = Some(at);
    self.ep.handle_suspect(from, suspect, at);
  }

  /// Begin a graceful leave; delegates to the membership endpoint.
  pub fn leave(&mut self, now: Instant) -> Result<(), Error> {
    self.leave_with(now, None)
  }

  /// [`leave`](Self::leave) with an explicit farewell payload reserved into
  /// every dead-self compound ahead of the ordinary queue drain (see
  /// [`Endpoint::leave_with`](crate::Endpoint::leave_with)).
  pub fn leave_with(&mut self, now: Instant, farewell: Option<Bytes>) -> Result<(), Error> {
    self.last_now = Some(now);
    self.ep.leave_with(now, farewell)
  }

  /// Initiate a direct application-level ping to `node`. Returns
  /// `Ok(`[`crate::PingId`]`)` — the correlation token the driver parks a
  /// waiter on, resolved by [`crate::event::Event::PingCompleted`] /
  /// [`crate::event::Event::PingFailed`] — or `Err(NotRunning)` once the node
  /// has left. Forwards to the inner membership [`Endpoint`].
  ///
  /// `ping` only queues a UDP gossip datagram (via `pending_transmits`) — it
  /// does not touch QUIC bridge state — so it is safe to call without a
  /// preceding `service_dials` / `flush_outbound`.
  // No last_now update: ping only enqueues a packet transmit; it touches no dial/bridge state that poll_timeout must immediately re-examine.
  #[inline]
  pub fn ping(
    &mut self,
    node: crate::Node<I, SocketAddr>,
    now: Instant,
  ) -> Result<crate::PingId, Error> {
    self.ep.ping(node, now)
  }

  /// Enqueue one or more directed unreliable user messages to `to`. Forwards
  /// to the inner membership [`Endpoint`]. Like `ping`, only touches the
  /// gossip `pending_transmits` queue, drained by `poll_memberlist_transmit`.
  ///
  /// Returns `Err` if any payload exceeds the configured `gossip_mtu` ceiling.
  #[inline]
  pub fn send_user_packets(&mut self, to: SocketAddr, payloads: &[Bytes]) -> Result<(), Error> {
    self.ep.send_user_packets(to, payloads)
  }

  /// Attempt EVERY parked dial — the fresh FIFO plus every peer bucket — and
  /// report both the outbound bridges it MINTED and every connection it created
  /// or mutated this call — see [`ServicedDials`].
  ///
  /// The liveness backstop the global tick and the `start_*` wrappers run: it
  /// drains [`Self::dial_pending`] (fresh, first-attempt order) then every
  /// [`Self::dial_parked`] bucket in insertion order, so an intent blocked on
  /// any peer — handshake pending, credit exhausted, or deadline elapsed — is
  /// re-attempted or retired here regardless of which per-peer event did or did
  /// not fire. The event-driven [`Self::service_peer_bucket`] handles the common
  /// case (one peer becomes ready) in O(that bucket); this handles the rest.
  ///
  /// A minted bridge lands on the DIALED peer's pooled connection via
  /// `get_or_dial`, which may be a connection OTHER than any one the caller is
  /// servicing — so the datagram path uses `minted_bridges` to pump exactly
  /// those bridges wherever they landed, without an O(all bridges) scan. Only
  /// the `dial_succeeded` success path mints. `touched_conns` additionally
  /// captures connections mutated WITHOUT minting a bridge (a cold dial whose
  /// `open(Bi)` returns `None` and requeues; an invalidated-intent reset), so
  /// the datagram path flushes their Initial/reset bytes and refreshes their
  /// deadline keys the same pass. The global tick ignores the return (its
  /// `collect_transmits` folds every connection regardless).
  fn service_dials(&mut self, now: Instant) -> ServicedDials {
    let mut minted: SmallVec<StreamId> = SmallVec::new();
    let mut touched = TouchedConns::new();
    // Sieve any DialRequested newly emitted by the inner endpoint into the
    // private `dial_pending` deque. Non-`DialRequested` events stay in the inner
    // endpoint's queue for the public `poll_event()` to observe.
    self.sieve_dial_events();
    // Drain the fresh FIFO (global first-attempt order) then every parked bucket
    // (insertion order). A still-blocked entry re-parks into `dial_parked`; both
    // `mem::take`s empty their source first, so on return `dial_parked` holds
    // exactly the intents that re-parked this pass.
    let pending = core::mem::take(&mut self.dial_pending);
    for entry in pending {
      self.process_dial_entry(entry, now, &mut minted, &mut touched);
    }
    let parked = core::mem::take(&mut self.dial_parked);
    for (_peer, bucket) in parked {
      for entry in bucket {
        self.process_dial_entry(entry, now, &mut minted, &mut touched);
      }
    }
    ServicedDials {
      minted_bridges: minted,
      touched_conns: touched.into_ordered(),
    }
  }

  /// Attempt only the dials parked on `peer` — the event-driven, scoped twin of
  /// [`Self::service_dials`]. Invoked when a per-peer readiness event fires (the
  /// peer's connection establishes, or it raises its MAX_STREAMS bidi limit), so
  /// exactly its blocked intents retry in O(that bucket) instead of scanning the
  /// whole dial queue on every such event.
  ///
  /// The bucket is removed whole, then each entry runs the same per-entry logic
  /// [`Self::service_dials`] applies; a still-blocked entry re-parks into
  /// `dial_parked[peer]` (recreating the bucket), so on return the bucket holds
  /// exactly the survivors and is absent when every entry cleared. Reports minted
  /// bridges and touched connections identically for the caller to pump and
  /// flush the same pass.
  fn service_peer_bucket(&mut self, peer: SocketAddr, now: Instant) -> ServicedDials {
    let mut minted: SmallVec<StreamId> = SmallVec::new();
    let mut touched = TouchedConns::new();
    if let Some(bucket) = self.dial_parked.shift_remove(&peer) {
      for entry in bucket {
        self.process_dial_entry(entry, now, &mut minted, &mut touched);
      }
    }
    ServicedDials {
      minted_bridges: minted,
      touched_conns: touched.into_ordered(),
    }
  }

  /// Attempt one dial intent, appending any minted bridge to `minted` and any
  /// created/mutated connection to `touched`. Shared verbatim by the full drain
  /// ([`Self::service_dials`]) and the scoped per-peer drain
  /// ([`Self::service_peer_bucket`]) so both apply identical semantics: deadline
  /// pre-check, `get_or_dial`, the `open(Bi)` three-way outcome, and — when the
  /// connection is still handshaking or the peer's bidi credit is exhausted — a
  /// re-park into [`Self::dial_parked`] keyed by the intent's target peer.
  fn process_dial_entry(
    &mut self,
    entry: PendingDial,
    now: Instant,
    minted: &mut SmallVec<StreamId>,
    touched: &mut TouchedConns,
  ) {
    #[cfg(test)]
    {
      self.counters.dial_entries_serviced = self.counters.dial_entries_serviced.saturating_add(1);
    }
    // Decompose AND mark attempted BEFORE the open attempt: if this
    // attempt requeues (handshake-blocked or credit-exhausted), the
    // re-pushed entry carries `attempted = true` so `poll_timeout` no
    // longer emits an immediate-due wake for it (the connection's own
    // `poll_timeout` and the entry's `deadline` drive the next service
    // tick; immediately re-firing would busy-loop a still-handshaking
    // connection).
    let PendingDial {
      id,
      peer,
      deadline,
      attempted,
    } = entry;
    // This entry left `dial_pending`: if it was still unattempted, release its
    // unit of the incremental `unattempted_dial_count`. A branch below that
    // requeues re-pushes with `attempted = true` (no bump), so the count
    // reaches exactly 0 once every unattempted entry has drained and is exact
    // on return — the O(1) source `refresh_immediate_due` reads. Reading
    // `attempted` here (not a blanket reset) also propagates any push-site
    // drift into the count so the cross-check catches it rather than masking
    // it at every drain.
    if !attempted {
      self.unattempted_dial_count = self.unattempted_dial_count.saturating_sub(1);
    }
    // `mem::take` removed this entry from `dial_pending`; drop its deadline
    // key. A branch that requeues re-registers it below, so after the loop
    // the `Dial` keys match the surviving intents exactly.
    self.deadline_index.set(TimerKey::Dial(id), None);
    // Retire the intent without opening anything on the pooled
    // connection if its own deadline has already elapsed.
    //
    // `quinn_proto::Streams::open(Dir::Bi)` inserts BOTH send AND recv
    // state for the new bidi stream. Letting `open` run for an expired
    // intent has no legitimate downstream consumer: `Endpoint::dial_succeeded`
    // (frozen) drops any intent whose deadline has elapsed and returns
    // `None`, so we would synthesise a fresh bidi stream on the pooled
    // connection that no `Bridge` ever owns and whose recv half is
    // unreachable. Resetting only the send half afterwards leaves the
    // recv half orphaned. The deadline pre-check routes the expired
    // intent through the FSM's `dial_failed` path BEFORE either half
    // is created, so no orphan state can exist.
    if now >= deadline {
      // Discard the staged kind and peer (the bridge was never
      // created, so the ExchangeCompleted reap path will never
      // observe this id) and surface a `Failed` completion for a
      // UserMessage or PushPull dial so the parked reliable-send /
      // join waiter resolves. Leaving entries stranded would leak
      // memory across every pre-deadline-expired dial. Matches the
      // pre-bridge-creation failure paths below.
      self.retire_failed_dial(
        id,
        StreamError::DialFailed("quic dial deadline elapsed".into()),
        now,
      );
      return;
    }
    // The membership address `peer` IS the wire `SocketAddr` (the
    // coordinator pins `A = SocketAddr` internally); the TLS verification
    // identity for this dial is resolved per-peer via the closure on
    // `QuicOptions` (default mode is cluster-uniform — the same string
    // for every peer — but operators with per-peer SAN certs supply a
    // closure that maps each `SocketAddr` to its expected identity).
    let addr = peer;
    let sni_arc = self.cfg.sni_for(&addr);
    match self.conns.get_or_dial(
      &mut self.quinn,
      now,
      self.cfg.client().clone(),
      addr,
      &sni_arc,
      self.cfg.max_quic_connections(),
    ) {
      Ok(ch) => {
        // Record every dialed connection as touched: `get_or_dial` may have
        // just created it (Initial bytes queued) or the `open(Bi)` below
        // mutates it (new stream state, or a reset on the invalidated-intent
        // path). The datagram-path caller flushes and re-indexes each touched
        // connection, even when no bridge is minted on it. O(1) dedup via the
        // accumulator's set — no O(touched²) `contains` scan across the pass.
        touched.insert(ch);
        if let Some(e) = self.conns.get_mut(ch) {
          match e.conn_mut().streams().open(Dir::Bi) {
            Some(sid) => match self.ep.dial_succeeded(id, now) {
              Some(stream) => {
                let reliable_max = self.ep.max_stream_frame_size();
                let mid = stream.id();
                self.bridges.insert(
                  mid,
                  Bridge::new(
                    stream,
                    ch,
                    sid,
                    #[cfg(compression)]
                    self.compression,
                    #[cfg(encryption)]
                    self.encryption.clone(),
                    reliable_max,
                    self.label.clone(),
                    self.skip_inbound_label_check,
                    true,
                  ),
                );
                // Mirror the mint into the per-connection bridge index so the
                // datagram path can pump this outbound bridge in O(conn), and
                // record it as minted this call so the datagram-path caller
                // pumps and flushes exactly the bridge on `ch` (which may differ
                // from the connection it is servicing).
                index_bridge_mint(&mut self.bridges, &mut self.bridges_by_conn, ch, mid);
                // Mirror into the `(ch, sid)` reverse index so this
                // connection's `StreamEvent::Finished`/`Stopped` resolve to
                // this bridge in O(1).
                self.bridge_by_conn_sid.insert((ch, sid), mid);
                // `inbound_bridge_count` replicates the old accept-gate scan,
                // `bridges.filter(|id| !pending_outbound_kinds.contains(id))`.
                // A dial that reached here WITHOUT a `pending_outbound_kinds`
                // entry — an internally-scheduled gossip push/pull, not a
                // driver-parked `start_*` exchange — was counted by that scan,
                // so it must be counted here too, or the reap-time decrement
                // (same `!contains` predicate) underflows. `start_*`-dialed
                // bridges are already in the map and are NOT counted, on either
                // side. This is the same predicate `deindex_reaped_bridge` uses.
                if !self.pending_outbound_kinds.contains_key(&mid) {
                  self.inbound_bridge_count += 1;
                }
                minted.push(mid);
              }
              None => {
                // Defense-in-depth: the deadline pre-check above
                // normally retires the intent before this branch is
                // reachable, but `Endpoint::dial_succeeded` is a
                // frozen API that may surface `None` for other
                // intent-invalidation reasons. `streams().open(Dir::Bi)`
                // already inserted BOTH send AND recv state on the
                // pooled connection; retiring only the send half leaves
                // the recv half orphaned and unreapable. Reset send +
                // stop recv so both halves are fully retired —
                // `SendStream::reset` queues RESET_STREAM and returns
                // `Err(ClosedStream)` harmlessly if the send half is
                // already gone; `RecvStream::stop` discards unread data
                // and queues STOP_SENDING with the same `Err(ClosedStream)`
                // guard.
                //
                // `retire_failed_dial` discards the staged kind/peer,
                // surfaces a `Failed` completion for a UserMessage /
                // PushPull dial (so a parked reliable-send / join
                // waiter resolves on this defense-in-depth path too),
                // and calls `dial_failed` — a no-op here because the
                // frozen `dial_succeeded` already consumed the intent,
                // but kept for uniformity with the other pre-bridge
                // failure sites.
                self.retire_failed_dial(
                  id,
                  StreamError::DialFailed(
                    "quic dial intent invalidated before bridge creation".into(),
                  ),
                  now,
                );
                if let Some(e) = self.conns.get_mut(ch) {
                  let conn = e.conn_mut();
                  // Ignoring Err: idempotent retirement —
                  // `Err(ClosedStream)` means the half is already
                  // gone.
                  let _ = conn
                    .send_stream(sid)
                    .reset(quinn_proto::VarInt::from_u32(0));
                  // Ignoring Err: same idempotent-retirement
                  // semantics as the send-half reset above.
                  let _ = conn.recv_stream(sid).stop(quinn_proto::VarInt::from_u32(0));
                }
              }
            },
            None => {
              // `quinn_proto::Streams::open(Dir::Bi) == None` has THREE
              // distinct causes (the call returns `None` when the
              // connection is closed OR when `next[Bi] >= max[Bi]`):
              //
              //   (1) `is_handshaking() == true` — the handshake has
              //       not finished, so the peer's initial-max-streams
              //       credit has not been granted yet. Common path for
              //       a fresh dial: the very first `DialRequested`
              //       arrives the same tick the connection is created,
              //       long before the handshake RTT completes. Requeue
              //       onto `dial_pending` while the intent's own
              //       deadline has not passed; the next tick retries
              //       `open(Bi)` once the handshake completes (the
              //       pooled connection is reused — no redial).
              //
              //   (2) `is_closed() == true` — the connection is
              //       `Closed`/`Draining`/`Drained` (the closed-before-
              //       drained pool window or a never-Established
              //       handshake-failed cache). `dial_failed`: consume
              //       the current intent. `get_or_dial` redials on the
              //       next push/pull/reliable-ping/user-message intent
              //       the application schedules (the cached closed
              //       handle for a once-Established peer triggers an
              //       explicit redial; a never-Established cache
              //       prevents a fresh-handshake storm against a
              //       genuinely-unreachable peer). The coordinator
              //       never repeatedly opens new
              //       handshakes against an unreachable peer inside a
              //       single intent's deadline.
              //
              //   (3) Established (not handshaking, not closed) — the
              //       peer's concurrent-bidi-stream credit
              //       (`initial_max_streams_bidi` / runtime
              //       `MAX_STREAMS`) is currently exhausted. A
              //       transient backpressure state lifted by a future
              //       `MAX_STREAMS` frame from the peer as inflight
              //       bidi streams reap. Requeue while the intent's
              //       own deadline has not passed — without this branch
              //       a steady-state cluster that pins its outbound
              //       concurrent-bidi-streams (e.g. coincident
              //       push/pulls + a reliable-ping fallback on the same
              //       pooled connection) would lose new reliable
              //       exchanges to permanent `dial_failed`.
              //
              // Re-parking into the private `dial_parked` bucket (NOT
              // `self.ep.requeue_event`) keeps the retry token private
              // so an external `poll_event` drain cannot pop it.
              let is_closed_now = self
                .conns
                .get(ch)
                .map(|c| c.conn_ref().is_closed())
                .unwrap_or(true);
              if is_closed_now {
                self.retire_failed_dial(
                  id,
                  StreamError::DialFailed("quic stream open: cached connection closed".into()),
                  now,
                );
              } else if now < deadline {
                // Re-park by TARGET PEER so the next readiness event on this
                // peer's connection (handshake completion, or a MAX_STREAMS
                // raise) services exactly this intent via
                // `service_peer_bucket`. `attempted = true`, so it no longer
                // contributes to the immediate-due wake.
                self.dial_parked.entry(peer).or_default().push(PendingDial {
                  id,
                  peer,
                  deadline,
                  attempted: true,
                });
                // Requeued (handshake-blocked or credit-exhausted): re-register
                // its deadline so the tick's exact-at-deadline retirement wake
                // still fires for the parked intent.
                self.deadline_index.set(TimerKey::Dial(id), Some(deadline));
              } else {
                self.retire_failed_dial(
                  id,
                  StreamError::DialFailed("quic stream open deadline elapsed".into()),
                  now,
                );
              }
            }
          }
        }
      }
      Err(conn::DialError::AtGlobalCap) => {
        // The global connection cap is reached: this new outbound peer gets
        // no connection. Count it against the same connection-cap metric the
        // inbound Initial path uses, and retire the intent through the
        // standard pre-bridge failure path (a Failed ExchangeCompleted for a
        // UserMessage / PushPull dial resolves the parked waiter).
        self.ep.metrics_mut().quic_connections_rejected += 1;
        self.retire_failed_dial(
          id,
          StreamError::DialFailed("quic connection table at capacity".into()),
          now,
        );
      }
      Err(conn::DialError::Connect(e)) => {
        self.retire_failed_dial(id, StreamError::DialFailed(e.to_string().into()), now);
      }
    }
  }
}

// The coordinator tick, scheduler arming, datagram/inbound handlers, and the
// bridge pump that fan out into probe/gossip work — drawing the gossip RNG.
impl<I, R> QuicEndpoint<I, R>
where
  R: Rng,
  I: crate::Id,
{
  /// Arm the periodic probe / gossip / push-pull schedulers. Forwards to
  /// [`Endpoint::start_scheduling`].
  #[inline]
  pub fn start_scheduling(&mut self, now: Instant) {
    self.ep.start_scheduling(now);
  }

  /// Seed an `Alive` state on the inner membership endpoint (typical
  /// bootstrap path: a harness teaching the coordinator about a known
  /// peer without going through a join push/pull).
  ///
  /// Pass-through to [`Endpoint::handle_alive`]. Sets `last_now`.
  pub fn handle_alive(
    &mut self,
    from: SocketAddr,
    alive: crate::typed::Alive<I, SocketAddr>,
    at: Instant,
  ) {
    self.last_now = Some(at);
    self.ep.handle_alive(from, alive, at);
  }

  /// Pump queued quinn outbound — including datagrams just handed to
  /// [`queue_unreliable_datagram`](Self::queue_unreliable_datagram) — into the
  /// [`poll_transmit`](Self::poll_transmit) queue at the current instant WITHOUT
  /// advancing any membership timer. A driver calls this after queuing
  /// unreliable datagrams so they flush on the SAME tick they were queued: a
  /// datagram carries a probe Ping whose timeout is armed in the same tick, and
  /// a one-tick send latency would let that timeout fire before the Ping ever
  /// left the host (a spurious failure). Idempotent and side-effect-free on
  /// membership state (no `Endpoint::handle_timeout`); the existing zero-time
  /// outbound flush the `start_*` paths already use.
  pub fn flush_outbound_transmits(&mut self, now: Instant) {
    self.flush_outbound(now);
  }

  /// Feed one decoded unreliable memberlist [`Message`](crate::typed::Message)
  /// (a frame the codec-owning layer unwrapped from a datagram surfaced by
  /// [`poll_memberlist_ingress`](Self::poll_memberlist_ingress)) into the
  /// inner membership endpoint.
  ///
  /// Pass-through to [`Endpoint::handle_packet`]; the composed unit's public
  /// ingress for the unreliable path is `handle_udp` → `poll_memberlist_ingress`
  /// → (codec decode) → `handle_packet`, never a direct call into the inner
  /// `Endpoint`.
  pub fn handle_packet(
    &mut self,
    from: SocketAddr,
    msg: crate::typed::Message<I, SocketAddr>,
    now: Instant,
  ) {
    self.ep.handle_packet(from, msg, now);
  }

  /// Step (2) of the per-tick order: pump every bridge's inbound + outbound
  /// halves, drain each non-terminal stream's endpoint-events into the
  /// `Endpoint`, and D1-drain-then-reap any bridge that turned terminal.
  ///
  /// Extracted so [`Self::flush_outbound`] can re-use the same bridge step
  /// after `service_dials` — a freshly-opened outbound bridge carries its
  /// request bytes in its FSM `Stream` output buffer, and a single pump is
  /// what moves those bytes into the quinn send stream so they emerge on
  /// the next [`Self::collect_transmits`].
  fn pump_bridges(&mut self, now: Instant) {
    let ids: MediumVec<StreamId> = self.bridges.keys().copied().collect();
    for id in ids {
      self.pump_one_bridge(id, now);
    }
  }

  /// Drain the pass-scoped ready-bridge queue [`Self::ready_bridges`]: pop each
  /// enqueued machine [`StreamId`] and pump it exactly once via the shared
  /// [`Self::pump_one_bridge`] (which clears its [`Bridge::queued`] flag at
  /// entry), returning the distinct set of [`ConnectionHandle`]s whose bridges
  /// it pumped so the caller flushes each one's owed transmits this same pass —
  /// an outbound bridge minted by `service_dials` can ride a connection other
  /// than the one being serviced. A since-reaped id no-ops (its
  /// [`Self::pump_one_bridge`] finds no bridge and returns `None`).
  ///
  /// Replaces the former all-bridges-on-`ch` pump on the datagram path: only the
  /// bridges a readiness trigger enqueued this pass are pumped, so a corrupted or
  /// replayed packet — which advances no stream state and fires no trigger —
  /// pumps zero, while under valid traffic the pumped set is exactly the bridges
  /// the arriving frames advanced.
  ///
  /// `#[cfg(not(test))]`: test builds drain via the post-acceptance-tracking
  /// [`Self::drain_ready_bridges_tracking`] instead, so this untracked variant is
  /// compiled only for production — mirroring how the global tick splits
  /// `pump_bridges` from `pump_bridges_tracking_post_acceptance`.
  #[cfg(not(test))]
  fn drain_ready_bridges(&mut self, now: Instant) -> SmallVec<ConnectionHandle> {
    let mut pumped_conns: SmallVec<ConnectionHandle> = SmallVec::new();
    while let Some(id) = self.ready_bridges.pop_front() {
      if let Some(ch) = self.pump_one_bridge(id, now) {
        if !pumped_conns.contains(&ch) {
          pumped_conns.push(ch);
        }
      }
    }
    pumped_conns
  }

  /// Pump one bridge `id`: its inbound + outbound halves, drain its
  /// endpoint-events into the `Endpoint`, and D1-drain-then-reap it if it turned
  /// terminal. The per-bridge body of [`Self::pump_bridges`], shared with the
  /// per-connection [`Self::drain_ready_bridges`] so both the global tick and the
  /// datagram path run identical bridge logic. Beyond that verbatim logic it
  /// maintains the two mandated indexes: [`Self::bridges_by_conn`] on reap and the
  /// `#[cfg(test)]` [`TestCounters::bridge_visits`] touch count.
  ///
  /// Returns the bridge's owning [`ConnectionHandle`] when the bridge was present
  /// (whether it survived or reaped), so the ready-queue drain can collect the
  /// distinct connections it pumped and flush their transmits; `None` when `id`
  /// named no live bridge (a stale ready-queue entry for an already-reaped
  /// stream). Clears the bridge's [`Bridge::queued`] flag at entry: the bridge is
  /// being serviced now, so a readiness trigger firing later this pass re-enqueues
  /// it for owed work that appears after the pump.
  fn pump_one_bridge(&mut self, id: StreamId, now: Instant) -> Option<ConnectionHandle> {
    // Split borrow: take the bridge out, operate, put back (or reap).
    if let Some(mut br) = self.bridges.remove(&id) {
      #[cfg(test)]
      {
        self.counters.bridge_visits = self.counters.bridge_visits.saturating_add(1);
      }
      // Serviced now: clear the ready-queue dedup flag so a later trigger this
      // pass can re-enqueue the bridge. The bridge's connection is invariant
      // across the pumps below, so capture it once for the return value and the
      // reap deindex.
      br.set_queued(false);
      let conn = br.ch();
      // `pump_in`/`pump_out` set the bridge `fatal` flag on a transport
      // error, so `is_terminal()` below drives the prompt reap; the
      // `#[must_use]` Results are consumed — terminality is the signal.
      let _ = br.pump_in(&mut self.conns, now);
      let _ = br.pump_out(&mut self.conns, now);
      // Drain endpoint-events EVERY tick (not only when terminal).
      // `drain_then_reap` also delivers the slot-gone notice (terminal
      // only); a non-terminal stream drains its payload events with the
      // SAME encode+load+flush but WITHOUT that notice.
      if br.is_terminal() {
        br.drain_then_reap(&mut self.ep, &mut self.conns, now);
        let outcome = Self::outcome_for_terminal(&br);
        let sid = br.sid();
        // Capture the bucket back-pointer BEFORE `drop(br)` — the O(1)
        // swap_remove deindex needs it.
        let conn_slot = br.conn_slot();
        // Reap AFTER drain: dropping the bridge frees its slot.
        drop(br);
        self.deindex_reaped_bridge(id, conn, sid, conn_slot);
        self.emit_exchange_completed(id, outcome);
      } else {
        br.drain_payload_only(&mut self.ep, &mut self.conns, now);
        // `drain_payload_only` may flip the bridge to terminal (e.g.
        // a `StreamCommand::Close` from an admission-rejected join sets
        // `fatal`); re-check terminality so the bridge D1-drains and
        // reaps in this SAME tick rather than holding the quinn bidi
        // stream until its exchange deadline.
        if br.is_terminal() {
          #[cfg(test)]
          {
            self.counters.bridges_terminalized_via_close_command = self
              .counters
              .bridges_terminalized_via_close_command
              .saturating_add(1);
          }
          br.drain_then_reap(&mut self.ep, &mut self.conns, now);
          let outcome = Self::outcome_for_terminal(&br);
          let sid = br.sid();
          // Capture the bucket back-pointer BEFORE `drop(br)`.
          let conn_slot = br.conn_slot();
          drop(br);
          self.deindex_reaped_bridge(id, conn, sid, conn_slot);
          self.emit_exchange_completed(id, outcome);
        } else {
          // Surviving bridge: refresh its deadline key before it moves back
          // into the table. This pump is the per-pass chokepoint for bridges —
          // it runs after every mint (`service_one_conn` / `service_dials`), so a
          // freshly-minted surviving bridge is registered here.
          let deadline = br.poll_timeout();
          self.bridges.insert(id, br);
          self.deadline_index.set(TimerKey::Bridge(id), deadline);
        }
      }
      Some(conn)
    } else {
      None
    }
  }

  /// Test-only variant of [`Self::pump_bridges`] that increments
  /// [`TestCounters::bridges_pumped_after_acceptance`] once for each bridge whose
  /// id is NOT in `pre_snapshot_ids` (i.e. inserted into `self.bridges`
  /// AFTER the snapshot was taken). Used by step (5.5) of [`Self::run_tick`]
  /// and the post-`service_quinn` second pump in [`Self::flush_outbound`] to
  /// prove the post-acceptance pump actually runs on every newly-inserted
  /// bridge — the negative-control regression test reverts the step (5.5)
  /// call site and the counter stays at zero.
  ///
  /// Pumping delegates to the shared [`Self::pump_one_bridge`], so production
  /// behaviour (the pump's effect on `self.bridges`, the inner `Endpoint`, and
  /// the indexes) is identical; the counter increment is the only added effect.
  #[cfg(test)]
  fn pump_bridges_tracking_post_acceptance(
    &mut self,
    now: Instant,
    pre_snapshot_ids: &HashSet<StreamId>,
  ) {
    let ids: Vec<StreamId> = self.bridges.keys().copied().collect();
    for id in ids {
      if self.bridges.contains_key(&id) && !pre_snapshot_ids.contains(&id) {
        self.counters.bridges_pumped_after_acceptance = self
          .counters
          .bridges_pumped_after_acceptance
          .saturating_add(1);
      }
      self.pump_one_bridge(id, now);
    }
  }

  /// Test-only variant of [`Self::drain_ready_bridges`] that increments
  /// [`TestCounters::bridges_pumped_after_acceptance`] once for each popped bridge
  /// present in `self.bridges` but NOT in `pre_snapshot_ids` — i.e. minted DURING
  /// this `service_connection` pass (an inbound accept, or a `service_dials`
  /// `open(Dir::Bi)` on establishment) and pumped the same pass. The datagram-path
  /// analog of [`Self::pump_bridges_tracking_post_acceptance`]; behaviour
  /// (draining, pumping, the returned distinct connections) is otherwise identical
  /// to the production variant.
  #[cfg(test)]
  fn drain_ready_bridges_tracking(
    &mut self,
    now: Instant,
    pre_snapshot_ids: &HashSet<StreamId>,
  ) -> SmallVec<ConnectionHandle> {
    let mut pumped_conns: SmallVec<ConnectionHandle> = SmallVec::new();
    while let Some(id) = self.ready_bridges.pop_front() {
      if self.bridges.contains_key(&id) && !pre_snapshot_ids.contains(&id) {
        self.counters.bridges_pumped_after_acceptance = self
          .counters
          .bridges_pumped_after_acceptance
          .saturating_add(1);
      }
      if let Some(ch) = self.pump_one_bridge(id, now) {
        if !pumped_conns.contains(&ch) {
          pumped_conns.push(ch);
        }
      }
    }
    pumped_conns
  }
  /// Inbound datagram from the one UDP socket.
  ///
  /// The `Quic` class is fully processed: the datagram is fed into quinn-proto's
  /// endpoint, any resulting `DatagramEvent` is routed, and a coordinator tick
  /// is run before returning. The `Memberlist` class is **buffered only** — the
  /// codec-owning driver MUST drain via [`Self::poll_memberlist_ingress`],
  /// decode each frame, feed every typed message via [`Self::handle_packet`],
  /// and then call [`Self::handle_timeout`] to advance time. Running
  /// [`Self::handle_timeout`] before the buffered memberlist datagrams are
  /// decoded and fed would risk same-instant probe / suspect timers firing
  /// before a just-arrived `Ack` / `Alive` is applied — a spurious fallback
  /// ping or false `Suspect` could fire even though the resolving message is
  /// already sitting in [`Self::poll_memberlist_ingress`]'s queue locally.
  /// The `Reject` class is dropped (the codec-owning layer surfaces the
  /// wire-level `DecodeError` on its own path).
  pub fn handle_udp(&mut self, from: SocketAddr, datagram: &[u8], now: Instant) {
    self.last_now = Some(now);
    match classify(datagram) {
      Class::Quic => {
        let mut scratch = Vec::new();
        let data = BytesMut::from(datagram);
        // `Endpoint::handle` returning `None` (a discarded malformed / truncated /
        // unmatched datagram) is skipped. When it returns an event,
        // `route_datagram_event` resolves it to the single connection this
        // datagram addresses — a freshly-accepted one, or a live handle a
        // `ConnectionEvent` targets — and `service_connection` services ONLY that
        // connection and its bridges (O(that connection), never the whole table).
        // A stateless `Response`, an over-cap / failed `accept`, and a
        // `ConnectionEvent` for an already-reaped handle resolve to `None`: their
        // owed outbound bytes are already queued to `out` and drain via
        // `poll_transmit` with no servicing pass owed. Membership time is NOT
        // advanced here (an inbound QUIC packet may carry an undecoded probe Ack);
        // only the driver's explicit `handle_timeout` advances it.
        if let Some(ev) = self
          .quinn
          .handle(now.into_std(), from, None, None, data, &mut scratch)
        {
          if let Some(ch) = self.route_datagram_event(ev, from, now, &scratch) {
            self.service_connection(ch, now);
          }
        }
      }
      Class::Memberlist => self.handle_memberlist_udp(from, datagram),
      Class::Reject => { /* drop; DecodeError is emitted by the codec path only */ }
    }
  }

  /// Timer tick from the driver.
  pub fn handle_timeout(&mut self, now: Instant) {
    self.last_now = Some(now);
    self.run_tick(now);
  }

  /// Initiate an outbound push/pull state exchange with `peer` and attempt
  /// the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_push_pull`] that ALSO drives
  /// `service_dials(now)` before returning, so the `DialRequested` the
  /// inner endpoint queues is sieved into the private `dial_pending` deque
  /// AND attempted (the bidi stream is opened if a pooled connection is
  /// already established; otherwise the entry stays in `dial_pending` with
  /// `attempted = true` and the next tick retries). Preferred entry point
  /// for the driver: a caller that goes through `endpoint_mut()` instead
  /// can only wake the dial via [`Self::poll_timeout`]'s immediate-due
  /// term — see that method's docs.
  ///
  /// Runs [`Self::flush_outbound`] after `service_dials` so the dial's
  /// Initial datagram (fresh dial) or the freshly-opened bridge's request
  /// bytes (pooled-Established dial) emerge on the very next
  /// [`Self::poll_transmit`] — a driver that uses only the public Sans-I/O
  /// poll surface (`poll_transmit` / `poll_timeout` / `handle_udp` /
  /// `handle_timeout`) sees the exchange progress without a same-instant
  /// `handle_timeout` pre-pump.
  pub fn start_push_pull(
    &mut self,
    peer: SocketAddr,
    kind: PushPullKind,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_push_pull(peer, kind, now);
    self
      .pending_outbound_kinds
      .insert(id, ExchangeKind::PushPull);
    self.pending_outbound_peers.insert(id, peer);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a reliable-stream fallback ping for probe `probe_seq` and
  /// attempt the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_reliable_ping`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-
  /// flush semantics. The `deadline` is the owning probe's single
  /// cumulative deadline (NOT an independent stream-timeout — the reliable
  /// fallback must race the indirect pings against the SAME deadline),
  /// forwarded unchanged. The inner method takes only the deadline so this
  /// wrapper accepts `now` separately: `service_dials` needs the real wall-
  /// clock instant (not the future deadline) and `last_now` must remain a
  /// known-past anchor.
  pub fn start_reliable_ping(
    &mut self,
    peer_id: I,
    peer_addr: SocketAddr,
    probe_seq: u32,
    deadline: Instant,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self
      .ep
      .start_reliable_ping(peer_id, peer_addr, probe_seq, deadline);
    self
      .pending_outbound_kinds
      .insert(id, ExchangeKind::ReliablePing);
    self.pending_outbound_peers.insert(id, peer_addr);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a one-way reliable user-message delivery to `peer` and
  /// attempt the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_user_message`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-
  /// flush semantics.
  pub fn start_user_message(
    &mut self,
    peer: SocketAddr,
    payload: Bytes,
    now: Instant,
  ) -> Result<StreamId, Error> {
    self.last_now = Some(now);
    // Propagate the inner lifecycle refusal: a Leaving/Left node starts no new
    // reliable user message and registers no outbound intent.
    let id = self.ep.start_user_message(peer, payload, now)?;
    self
      .pending_outbound_kinds
      .insert(id, ExchangeKind::UserMessage);
    self.pending_outbound_peers.insert(id, peer);
    self.service_dials(now);
    self.flush_outbound(now);
    Ok(id)
  }

  /// The fixed per-tick step order (load-bearing — see module docs).
  ///
  /// Step (2) (drain each non-terminal stream's endpoint-events into the
  /// `Endpoint`) MUST strictly precede step (3) (`ep.handle_timeout`): a
  /// reliable-fallback ping ack delivered on the same tick the probe
  /// cumulative deadline expires is carried by the stream's last
  /// `poll_endpoint_event`; draining it after the probe timeout would lose it
  /// and wrongly Suspect a live peer. Do not reorder.
  ///
  /// Step (5.5) (a second `pump_bridges(now)` call) is the strict-poll
  /// self-sufficiency seam for INBOUND accepts and freshly-opened OUTBOUND
  /// bridges. Step (4) (`service_quinn`) inserts new inbound bridges via
  /// its `accept(Dir::Bi)` loop, and step (5) (`service_dials`) inserts new
  /// outbound bridges via `streams().open(Dir::Bi)`. Both insertions happen
  /// AFTER step (2)'s `pump_bridges` already ran — so without step (5.5)
  /// those bridges are never pumped this tick. The next coordinator wake
  /// under strict poll-surface driving comes from [`Self::poll_timeout`],
  /// whose `min` only covers transport timers and the bridge's own
  /// exchange deadline: `quinn_proto::Connection::poll_timeout` returns
  /// `self.timers.next_timeout()` (loss detection / idle / close /
  /// KeyDiscard / KeepAlive), and app-read readiness is NOT advertised as
  /// a transport timer. The next wake is therefore the bridge's exchange
  /// deadline; by then `Stream::handle_data` rejects the buffered request
  /// as timed out and the exchange fails. Step (5.5) closes that gap by
  /// pumping every newly-inserted bridge in the same tick its first data
  /// arrived — mirroring the same strict-poll self-sufficiency invariant
  /// applied by the `start_*` zero-time flush (Case A pump after a
  /// pooled-Established dial) and the connection-loss inline drain.
  ///
  /// `pump_bridges` is idempotent on already-pumped bridges: `pump_in`
  /// drains every available chunk and the next call observes `Blocked`
  /// from `quinn_proto::RecvStream::read`; `pump_out` flushes `pending_out`
  /// and exhausts `Stream::poll_transmit`, and the next call finds both
  /// empty; `drain_payload_only` runs an empty `Stream::poll_endpoint_event`
  /// loop; `drain_then_reap` only fires on a terminal bridge, which is
  /// removed from `self.bridges` after the first call. The second pump
  /// is therefore a no-op on every bridge that already ran in step (2).
  fn run_tick(&mut self, now: Instant) {
    self.tick(now, true);
  }

  /// Service exactly the one connection `ch` an inbound datagram addressed, and
  /// its bridges — the per-connection analog of the global `tick(now, false)`,
  /// bounding all inbound-datagram work to O(this connection + its bridges) so a
  /// flood at one live CID can never force an O(all connections + bridges) pass.
  ///
  /// Membership timers are NOT advanced here (as with the former datagram tick):
  /// a QUIC packet may carry application datagrams (probe Acks, Alives) the driver
  /// has not yet decoded; firing the membership probe or suspicion deadline before
  /// the driver drains and decodes `mem_ingress` would mark a peer Suspect/failed
  /// even though its Ack already arrived. Membership time advances ONLY through
  /// the driver's explicit `handle_timeout`, giving the QUIC datagram ingress path
  /// the same property the plain-UDP path (`handle_memberlist_udp`) already has.
  ///
  /// A connection whose own transport timer, or a bridge whose exchange deadline,
  /// needs future service is still woken: every such deadline lives in
  /// [`Self::deadline_index`], so `poll_timeout` folds it in and the driver's next
  /// `handle_timeout` runs the global tick — nothing is stranded by servicing only
  /// the addressed connection here.
  fn service_connection(&mut self, ch: ConnectionHandle, now: Instant) {
    #[cfg(test)]
    {
      self.counters.quic_inbound_servicings =
        self.counters.quic_inbound_servicings.saturating_add(1);
    }
    // Snapshot `ch`'s bridges so the pump after `service_one_conn` counts the
    // ones this pass mints (inbound accepts) as post-acceptance pumps — the
    // datagram-path twin of the global tick's step (5.5).
    #[cfg(test)]
    let pre_service_ids: HashSet<StreamId> = self
      .bridges_by_conn
      .get(&ch)
      .map(|ids| ids.iter().copied().collect())
      .unwrap_or_default();
    // (a) Drive `ch`: apply its deferred feedback + timers, drain its `poll()`
    // (accepting inbound bridges, routing per-stream readiness events, reaping its
    // bridges on a connection-level loss). Its accept loop enqueues each fresh
    // inbound bridge (T1) and its `poll()` drain enqueues each bridge a
    // `Readable`/`Writable`/`Finished`/`Stopped` made ready (T2–T5). It returns
    // the readiness marks (establishment, credit restore) consumed by step (c).
    let marks = self.service_one_conn(ch, now);
    // (b) Drain the ready queue: pump exactly the bridges `service_one_conn` made
    // ready this pass — never every bridge on `ch` — so their first buffered
    // request/response bytes reach the quinn send stream and any terminal bridge
    // reaps the same pass. A corrupted or replayed packet fires no trigger, so
    // this drains nothing. The drain returns the distinct connections it pumped;
    // accumulate them (plus the dials' touched connections below) to flush after
    // step (d).
    #[cfg(test)]
    let mut to_flush = self.drain_ready_bridges_tracking(now, &pre_service_ids);
    #[cfg(not(test))]
    let mut to_flush = self.drain_ready_bridges(now);
    // (c) If `ch` became ready this pass — its handshake completed, or its peer
    // raised its MAX_STREAMS bidi limit — attempt ONLY the dials parked on that
    // peer, never the whole dial queue. A pooled push/pull or reliable-ping
    // intent requeued while `ch` was still handshaking (or blocked on this peer's
    // exhausted bidi credit) opens the instant `ch` becomes ready, without paying
    // an O(all parked dials) scan on this per-datagram, non-attacker-floodable
    // transition (a corrupted packet establishes nothing and raises no credit).
    // Both readiness marks resolve to the same target — this connection's peer —
    // so the bucket is serviced at most once even when both fired the same pass.
    //
    // `service_peer_bucket` drains only `dial_parked[peer]`. Each entry mints a
    // bridge on `peer`'s pooled connection (`ch` itself here) or — the peer's
    // credit still exhausted — re-parks; a redial onto a repointed handle can
    // also touch a connection without minting. So enqueue the bridges it reports
    // minting (T1) and drain them, then flush every connection it reports
    // TOUCHING so each one's first request bytes / Initial / reset bytes reach
    // the `out` wire AND its deadline key is registered THIS pass. Without it a
    // strict-poll driver would sleep until the next global tick, past the first
    // datagram — the same strict-poll self-sufficiency `flush_outbound` documents.
    // `ch` itself is collected by step (d).
    //
    // Prefer the captured credit-restore peer (robust to `ch` being reaped this
    // pass); fall back to resolving the peer for a pure establishment.
    let service_peer = marks.credit_restored_peer.or_else(|| {
      marks
        .established_transition
        .then(|| self.conns.get(ch).map(|e| e.peer()))
        .flatten()
    });
    if let Some(peer) = service_peer {
      let ServicedDials {
        minted_bridges,
        touched_conns,
      } = self.service_peer_bucket(peer, now);
      // Enqueue each freshly-minted outbound bridge (T1, outbound twin of the
      // inbound-accept enqueue) so the drain below pumps its first request bytes
      // into its quinn send stream before its owning connection is collected.
      for mid in minted_bridges {
        enqueue_ready_bridge(&mut self.bridges, &mut self.ready_bridges, mid);
      }
      #[cfg(test)]
      let pumped = self.drain_ready_bridges_tracking(now, &pre_service_ids);
      #[cfg(not(test))]
      let pumped = self.drain_ready_bridges(now);
      for mc in pumped {
        if !to_flush.contains(&mc) {
          to_flush.push(mc);
        }
      }
      // Flush + re-index every connection the dials touched — the minted-bridge
      // owners AND the mutated-but-bridgeless ones (a redial Initial, an
      // invalidated-intent reset).
      for mc in touched_conns {
        if !to_flush.contains(&mc) {
          to_flush.push(mc);
        }
      }
    }
    // (d) Reap `ch` if it drained this pass — a connection-level loss, or a
    // Closed→Drained stateless-reset transition delivered as a `ConnectionEvent`
    // (which the former progress gate wrongly treated as inert, stranding the
    // reap). Reaping frees its slab entry and its global-cap slot in this same
    // call. Otherwise collect exactly its owed transmits and refresh its deadline
    // key. Mirrors `finalize_tick`'s reap-then-collect ordering, scoped to `ch`.
    if self.conns.reap_if_drained(&mut self.quinn, ch) {
      self.deadline_index.set(TimerKey::Conn(ch), None);
      self.conns_with_pending_events.remove(&ch);
      self.bridges_by_conn.remove(&ch);
    } else {
      self.collect_conn_transmits(ch, now);
    }
    // Flush every OTHER connection a pumped bridge rode or a dial touched this
    // pass so its owed bytes reach `out` and its deadline key refreshes now; `ch`
    // is already collected above. (`ch`'s own ready-pumped bridges ride `ch`, so
    // step (d)'s collect covers them.)
    for mc in to_flush {
      if mc != ch {
        self.collect_conn_transmits(mc, now);
      }
    }
    // Pass-end invariant: the pass-scoped ready queue is drained empty and no
    // live bridge still flags queued.
    self.debug_assert_ready_drained();
  }

  /// Shared tick body. `advance_membership_time` gates step (3)
  /// (`Endpoint::handle_timeout`): true for the driver's explicit timer tick.
  fn tick(&mut self, now: Instant, advance_membership_time: bool) {
    // (1) inbound feed already done in `handle_udp` before this tick.
    // (2) pump bridges + drain stream endpoint-events into the Endpoint.
    #[cfg(test)]
    let pre_step2_ids: HashSet<StreamId> = self.bridges.keys().copied().collect();
    self.pump_bridges(now);
    // (3) THEN memberlist timers (probe cumulative-deadline, suspicion).
    if advance_membership_time {
      #[cfg(test)]
      {
        self.counters.membership_time_advances =
          self.counters.membership_time_advances.saturating_add(1);
      }
      self.ep.handle_timeout(now);
    }
    // (4) quinn connection timers + accept new bidi streams + transmit. The
    // ready-peer marks it returns need no action here: step (5) full-drains every
    // parked bucket unconditionally, so a bucket a readiness event unblocked is
    // serviced there regardless.
    let _ready_peers = self.service_quinn(now);
    // (5) Dial requests emitted by (3) or by accept-events, plus every parked
    // bucket (the liveness backstop for expiry and handshake-failure retirement).
    self.service_dials(now);
    // (5.5) Pump bridges inserted by (4) and (5) this same tick — see
    // method docstring above for the strict-poll self-sufficiency rationale.
    #[cfg(test)]
    self.pump_bridges_tracking_post_acceptance(now, &pre_step2_ids);
    #[cfg(not(test))]
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Zero-time outbound flush invoked from the high-level `start_*` APIs
  /// AFTER `service_dials`. Runs the shared [`Self::run_tick`] tail
  /// (bridge pump + `service_quinn` + drained-reap + `collect_transmits`)
  /// WITHOUT step (3) (`Endpoint::handle_timeout`).
  ///
  /// Step (3) is deliberately skipped: memberlist timers (probe cumulative-
  /// deadline, suspicion, gossip / push-pull schedulers) advance solely
  /// through the driver's explicit [`Self::handle_timeout`], which fires
  /// AFTER the driver has drained [`Self::poll_memberlist_ingress`],
  /// decoded each frame, and fed each typed message via
  /// [`Self::handle_packet`]. Advancing time inside a `start_*` call would
  /// fire same-instant probe / suspect / gossip / push-pull schedulers
  /// BEFORE a just-arrived (still-buffered) `Ack` / `Alive` is decoded and
  /// applied — the same property [`Self::handle_udp`] protects on the
  /// `Class::Memberlist` ingress path.
  ///
  /// Bridge step (2) is included because for an already-Established pooled
  /// connection `service_dials` opens a fresh bidi stream and inserts a new
  /// `Bridge` carrying the encoded request bytes in its FSM `Stream` output
  /// buffer; the bytes only reach the quinn send stream when `pump_out`
  /// runs. Without this same-instant pump, the bytes sit inside the bridge
  /// and the next [`Self::collect_transmits`] returns empty — a driver that
  /// uses only `poll_transmit` / `poll_timeout` / `handle_udp` would see
  /// the dial's Initial (or the bridge's request bytes) only on the NEXT
  /// `handle_timeout` cycle, advancing virtual time before the exchange
  /// emits its first datagram.
  ///
  /// `Connection::poll_transmit` likewise only emerges through
  /// [`Self::collect_transmits`]; running `service_quinn` + `collect_transmits`
  /// here puts a fresh dial's Initial onto the outbound queue at the same
  /// instant the `start_*` returns.
  ///
  /// `service_dials` is run BY THE CALLER (the `start_*` wrapper) before
  /// this method, mirroring `run_tick`'s ordering: the dial is processed,
  /// then its outbound side-effects flush in the same call.
  ///
  /// A second `pump_bridges(now)` runs AFTER `service_quinn` to pump any
  /// inbound bridges its `accept(Dir::Bi)` loop just inserted: `start_*`
  /// is called from arbitrary points in the driver's loop, and a peer's
  /// data may have arrived since the prior `handle_udp` (a Bi stream that
  /// `service_quinn` accepts inside this `flush_outbound`). Without this
  /// second pump, that newly-accepted inbound bridge's first data isn't
  /// fed into `Bridge::pump_in` this tick, and a strict-poll driver next
  /// wakes at the bridge's exchange deadline — at which point
  /// `Stream::handle_data` rejects the buffered request as timed out. See
  /// [`Self::run_tick`]'s docstring for the full strict-poll self-
  /// sufficiency rationale; the second pump's idempotency on already-
  /// pumped bridges is the same property documented there.
  ///
  /// Unlike the global tick, this method owns no `service_dials`. So it services
  /// exactly the parked buckets whose peers `service_quinn` reported becoming
  /// ready during THIS flush — a connection establishing, or a peer raising its
  /// MAX_STREAMS bidi limit — instead of leaving them to the next tick. Those
  /// buckets' minted bridges and touched connections are flushed by the full
  /// second `pump_bridges` and `finalize_tick`'s `collect_transmits` below.
  fn flush_outbound(&mut self, now: Instant) {
    #[cfg(test)]
    let pre_first_pump_ids: HashSet<StreamId> = self.bridges.keys().copied().collect();
    self.pump_bridges(now);
    let ready_peers = self.service_quinn(now);
    for peer in ready_peers {
      // Ignoring the ServicedDials: the full second `pump_bridges` and
      // `finalize_tick`'s `collect_transmits` below flush every bridge minted and
      // connection touched by these bucket drains, so the per-pass side-effect
      // report the datagram path consumes is not needed here.
      let _ = self.service_peer_bucket(peer, now);
    }
    #[cfg(test)]
    self.pump_bridges_tracking_post_acceptance(now, &pre_first_pump_ids);
    #[cfg(not(test))]
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Service every connection and collect the DISTINCT peers whose parked dials
  /// a per-connection readiness event (establishment, or a MAX_STREAMS bidi
  /// raise) unblocked this pass, in first-observed order. Both `flush_outbound`
  /// (which owns no `service_dials`) and the global tick call this; the tick
  /// discards the result because its following `service_dials` full-drains every
  /// bucket regardless, while `flush_outbound` services exactly these buckets so
  /// a readiness observed during a `start_*` flush is not left to the next tick.
  fn service_quinn(&mut self, now: Instant) -> SmallVec<SocketAddr> {
    let mut ready_peers: SmallVec<SocketAddr> = SmallVec::new();
    for ch in self.conns.iter_handles() {
      let marks = self.service_one_conn(ch, now);
      // Establishment unblocks the dials parked on THIS connection's peer;
      // resolve it while `ch` is still in hand (it may have been reaped during
      // the pass, in which case there is no bucket to service — the tick is the
      // backstop). A credit raise carries its peer as a value already.
      let established_peer = marks
        .established_transition
        .then(|| self.conns.get(ch).map(|c| c.peer()))
        .flatten();
      for peer in [marks.credit_restored_peer, established_peer]
        .into_iter()
        .flatten()
      {
        if !ready_peers.contains(&peer) {
          ready_peers.push(peer);
        }
      }
    }
    ready_peers
  }

  /// Service exactly one connection `ch`: apply its deferred feedback, drive its
  /// timers, drain its `poll()` (accepting inbound bidi streams into bridges,
  /// routing per-stream Finished/Stopped, reaping its bridges on a
  /// connection-level loss), drain its endpoint-events into the `Endpoint`, and
  /// extract its inbound datagrams. The per-connection body of
  /// [`Self::service_quinn`], shared with the per-datagram
  /// [`Self::service_connection`] so both the global tick and the datagram path
  /// run identical connection logic. Beyond that verbatim logic it maintains
  /// [`Self::bridges_by_conn`] at its bridge mint/reap sites and the
  /// `#[cfg(test)]` [`TestCounters::connection_visits`] touch count.
  ///
  /// Returns the [`ServiceMarks`] this pass observed — whether the connection
  /// established (a `false -> true` establishment flip) and whether the peer
  /// raised its bidi credit (a `StreamEvent::Available { dir: Bi }`). The caller
  /// consumes them AFTER the connection borrow drops to service the peer's parked
  /// dials; they cannot be acted on in place because the parked-dial servicing
  /// re-borrows `self.conns` through `get_or_dial`.
  fn service_one_conn(&mut self, ch: ConnectionHandle, now: Instant) -> ServiceMarks {
    let Some(e) = self.conns.get_mut(ch) else {
      return ServiceMarks::default();
    };
    #[cfg(test)]
    {
      self.counters.connection_visits = self.counters.connection_visits.saturating_add(1);
    }
    // Sample the establishment observation BEFORE the first `conn_mut()` below,
    // which lazily flips the sticky flag when the handshake has completed. The
    // `false -> true` transition across this pass is the establishment mark.
    let established_before = e.established_at_least_once();
    // Apply this connection's one-tick-deferred feedback BEFORE any
    // other poll on it — same shape as quinn-proto's reference async
    // driver's per-connection channel rx, where `process_conn_events`
    // is called once per scheduling iteration on the connection task
    // and `Endpoint::handle_events` produces the corresponding
    // `ConnectionEvent::Proto` messages on the SAME tick's
    // `Endpoint::handle_event` return. Materialise into a `Vec` so
    // the iterator releases its borrow of `e.pending_events` before
    // the `handle_event` mutable borrow.
    let pending: Vec<quinn_proto::ConnectionEvent> = e.take_pending_events().collect();
    for conn_ev in pending {
      e.conn_mut().handle_event(conn_ev);
    }
    e.conn_mut().handle_timeout(now.into_std());
    let mut lost = false;
    // Set when this pass drains a `StreamEvent::Available { dir: Bi }` — the peer
    // raised its MAX_STREAMS bidi limit, so a dial requeued on this peer's
    // exhausted bidi credit can now open. Consumed after the borrow drops.
    let mut credit_restored = false;
    while let Some(ev) = e.conn_mut().poll() {
      match ev {
        quinn_proto::Event::ConnectionLost { .. } => {
          // The connection (not an individual stream) failed; the per-stream
          // pumps cannot observe this. Defer marking until the `poll()` loop
          // ends so `e`'s mutable borrow of `conns` is dropped first.
          lost = true;
        }
        quinn_proto::Event::Stream(quinn_proto::StreamEvent::Opened { dir: Dir::Bi }) => {
          // `StreamEvent::Opened` is an idempotent signal ("one or more
          // streams opened"), not a per-stream event, so accept until the
          // peer's bidi backlog is drained — otherwise concurrently opened
          // inbound exchanges are stranded with no further wake-up.
          //
          // Cross-connection inbound-stream cap. Each accepted bidi stream
          // mints a Bridge that pins up to ~3x the max reliable frame size, so
          // admission-gate every inbound bridge against the QUIC-specific
          // `QuicOptions::max_inbound_streams` (a bounded nonzero default)
          // BEFORE minting — the QUIC twin of the stream coordinator's
          // `accept_connection` gate (`streams::StreamEndpoint`, which counts
          // `exchanges.filter(|m| !m.outbound)`). The QUIC ceiling is its own
          // option, not the shared TCP/TLS `EndpointOptions::max_inbound_streams`
          // (which defaults to unlimited), so a default QUIC endpoint is bounded
          // without changing TCP/TLS behaviour. This bounds inbound bridge state
          // ACROSS all connections; quinn's per-connection
          // `max_concurrent_bidi_streams` is a separate, per-connection limit.
          // An outbound bridge is registered in `pending_outbound_kinds` for
          // the life of its exchange and an accepted (inbound) bridge never is
          // (see that field's docs), so the inbound population is exactly the
          // bridges absent from that map — tracked incrementally as
          // `inbound_bridge_count`. Read that O(1) count instead of filtering
          // the whole bridge table on every peer-driven `Opened` (one datagram
          // could otherwise force an O(all bridges) fold); the mint below bumps
          // it in place, so a later connection's accept loop this same pass sees
          // the updated total — the cap is ACROSS all connections.
          let max_inbound = self.cfg.max_inbound_streams();
          while let Some(sid) = e.conn_mut().streams().accept(Dir::Bi) {
            let peer = e.peer();
            // At the inbound ceiling: refuse this stream instead of minting a
            // bridge. Reset both halves so the peer is notified and quinn
            // releases the stream slot, and bump `inbound_streams_rejected`.
            // Ignoring Err: `ClosedStream` means the half is already gone,
            // which is the desired end state.
            if max_inbound.is_some_and(|max| self.inbound_bridge_count >= max) {
              self.ep.metrics_mut().inbound_streams_rejected += 1;
              let _ = e
                .conn_mut()
                .send_stream(sid)
                .reset(quinn_proto::VarInt::from_u32(0));
              let _ = e
                .conn_mut()
                .recv_stream(sid)
                .stop(quinn_proto::VarInt::from_u32(0));
              continue;
            }
            let Some(stream) = self.ep.accept_stream(peer, now) else {
              // Leaving/Left: admit no new inbound reliable stream. Reset both
              // halves of the just-accepted QUIC stream so the peer is notified
              // and the connection's stream slot is released instead of left
              // orphaned with no Bridge to own it. Ignoring Err: `ClosedStream`
              // means the half is already gone, which is the desired end state.
              let _ = e
                .conn_mut()
                .send_stream(sid)
                .reset(quinn_proto::VarInt::from_u32(0));
              let _ = e
                .conn_mut()
                .recv_stream(sid)
                .stop(quinn_proto::VarInt::from_u32(0));
              continue;
            };
            let id = stream.id();
            let reliable_max = self.ep.max_stream_frame_size();
            self.bridges.insert(
              id,
              Bridge::new(
                stream,
                ch,
                sid,
                #[cfg(compression)]
                self.compression,
                #[cfg(encryption)]
                self.encryption.clone(),
                reliable_max,
                self.label.clone(),
                self.skip_inbound_label_check,
                false,
              ),
            );
            // Mirror the mint into both bridge indexes (each borrows only its own
            // field, disjoint from the live `e`/`self.conns` borrow), and bump
            // the incremental inbound population this accept gate reads. An
            // accepted bridge is never in `pending_outbound_kinds`, so the guard
            // is always true here — but keeping the SAME `!contains` predicate the
            // reap-time decrement uses makes the count provably balanced no matter
            // how the id spaces evolve.
            index_bridge_mint(&mut self.bridges, &mut self.bridges_by_conn, ch, id);
            self.bridge_by_conn_sid.insert((ch, sid), id);
            if !self.pending_outbound_kinds.contains_key(&id) {
              self.inbound_bridge_count += 1;
            }
            #[cfg(test)]
            {
              self.counters.max_inbound_bridges_live = self
                .counters
                .max_inbound_bridges_live
                .max(self.inbound_bridge_count);
            }
            // T1 (inbound mint): a freshly-accepted inbound bridge already holds
            // its first request bytes in quinn's per-stream assembler (this
            // datagram delivered them), yet a new remote stream's first frames
            // set only the coalesced `Opened` flag and emit no `Readable`, so no
            // later event re-announces those bytes. Enqueue it here so the pass
            // drain pumps the buffered request the same pass it was accepted.
            // Disjoint from the live `e`/`self.conns` borrow — see
            // `enqueue_ready_bridge`.
            enqueue_ready_bridge(&mut self.bridges, &mut self.ready_bridges, id);
          }
        }
        quinn_proto::Event::Stream(quinn_proto::StreamEvent::Readable { id: sid }) => {
          // T2 (readable): new inbound data, a peer FIN, or a peer RESET for a
          // known stream. Resolve the owning bridge in O(1) via the `(ch, sid)`
          // reverse index and enqueue it so the pass drain feeds the bytes
          // through `pump_in`. A stale event for a refused/reaped stream resolves
          // to no bridge and no-ops.
          if let Some(&mid) = self.bridge_by_conn_sid.get(&(ch, sid)) {
            enqueue_ready_bridge(&mut self.bridges, &mut self.ready_bridges, mid);
          }
        }
        quinn_proto::Event::Stream(quinn_proto::StreamEvent::Writable { id: sid }) => {
          // T3 (writable): send-side backpressure released for this stream (a
          // MAX_STREAM_DATA raise, or a connection-window / ACK relax surfaced by
          // `poll()`). Enqueue the owning bridge so the pass drain retries its
          // blocked `pending_out` flush in `pump_out`.
          if let Some(&mid) = self.bridge_by_conn_sid.get(&(ch, sid)) {
            enqueue_ready_bridge(&mut self.bridges, &mut self.ready_bridges, mid);
          }
        }
        quinn_proto::Event::Stream(quinn_proto::StreamEvent::Finished { id: sid }) => {
          // Peer ack'd our FIN — quinn-proto's `SendState` for this
          // stream has reached `DataRecvd`. Route to the owning
          // bridge so it transitions `Active -> SendClosed` (or
          // `RecvClosed -> BothClosed`). The bridge's terminality
          // criterion is `LinkState::BothClosed | Failed(_)`, so
          // this transition is the load-bearing send-half retirement
          // observable — not `SendStream::finish()`'s return.
          //
          // **Identity is the compound `(ConnectionHandle, QuicSid)`.**
          // quinn-proto's `StreamId` is per-connection — its bottom two bits
          // encode initiator + direction and the remaining counter is
          // per-connection, so two pooled peer connections both hold their
          // first bidi stream as sid `0` and a sid-only match would misroute
          // across connections. The `bridge_by_conn_sid` reverse index resolves
          // the owning bridge in O(1), so a peer-driven event never scans the
          // bridge table.
          #[cfg(test)]
          {
            self.counters.bridge_scan_visits = self.counters.bridge_scan_visits.saturating_add(1);
          }
          if let Some(&mid) = self.bridge_by_conn_sid.get(&(ch, sid)) {
            if let Some(br) = self.bridges.get_mut(&mid) {
              br.observe_send_fin();
            }
            // T4: the send-FIN observation may complete `BothClosed`; enqueue so
            // the pass drain runs the terminal check and terminalize-and-reaps
            // this same pass rather than deferring to the bridge deadline.
            enqueue_ready_bridge(&mut self.bridges, &mut self.ready_bridges, mid);
          }
        }
        quinn_proto::Event::Stream(quinn_proto::StreamEvent::Stopped {
          id: sid,
          error_code,
        }) => {
          // Peer sent STOP_SENDING for our send half. quinn already
          // transitioned our `SendState` to `ResetSent`; we additionally
          // retire the recv half (idempotent) so the bridge becomes
          // recv-clean by the time its `Failed(Transport)` phase
          // reaps. Retirement is inline on `e.conn_mut()` because we
          // already hold the `&mut Connection` borrow for the
          // `poll()` drain.
          //
          // Identity = `(ch, sid)` — see the `Finished` arm above.
          //
          // Ignoring Err: idempotent retirement — `Err(ClosedStream)`
          // means the half is already gone.
          let _ = e
            .conn_mut()
            .send_stream(sid)
            .reset(quinn_proto::VarInt::from_u32(0));
          let _ = e
            .conn_mut()
            .recv_stream(sid)
            .stop(quinn_proto::VarInt::from_u32(0));
          // O(1) reverse-index lookup — see the `Finished` arm for why the
          // compound `(ch, sid)` key, and never a bridge-table scan.
          #[cfg(test)]
          {
            self.counters.bridge_scan_visits = self.counters.bridge_scan_visits.saturating_add(1);
          }
          if let Some(&mid) = self.bridge_by_conn_sid.get(&(ch, sid)) {
            if let Some(br) = self.bridges.get_mut(&mid) {
              br.fail_stopped_already_retired(error_code);
            }
            // T5: the send half is now `ResetSent` and the bridge failed;
            // enqueue so the pass drain reaps it this same pass.
            enqueue_ready_bridge(&mut self.bridges, &mut self.ready_bridges, mid);
          }
        }
        quinn_proto::Event::Stream(quinn_proto::StreamEvent::Available { dir: Dir::Bi }) => {
          // The peer raised its bidi MAX_STREAMS limit (a received MAX_STREAMS
          // frame lifting the count we may open). An outbound dial requeued
          // because this peer's concurrent-bidi-stream credit was exhausted can
          // now open; mark the peer so the caller services its parked bucket once
          // after this borrow drops. Edge-triggered on the raise — if another
          // dial consumes the restored credit first, the loser simply re-parks
          // and waits for the next raise or the tick backstop (no busy loop,
          // because a re-parked entry stays `attempted`). An `Available` for a
          // peer with no parked bucket is a no-op there.
          credit_restored = true;
        }
        _ => {}
      }
    }
    // Drain every endpoint-facing event the connection has queued and
    // route it back through `Endpoint::handle_event`; queue any
    // returned `ConnectionEvent` onto the SAME connection for delivery
    // on the NEXT `service_quinn` iteration. quinn-proto's polling
    // contract requires callers to drain all four polling methods
    // every progress step. Omitting `poll_endpoint_events` strands
    // `NeedIdentifiers` / `ResetToken` / `RetireConnectionId` inside
    // the connection and breaks the endpoint flows they drive: CID
    // issuance via `Endpoint::send_new_identifiers`, peer
    // stateless-reset-token registration, and CID retirement.
    // Placed after the application-event `poll()` loop above so the
    // existing `Opened`/`ConnectionLost` handling is unchanged in
    // observable behaviour.
    //
    // `EndpointEventInner::Drained` is filtered out here and forwarded
    // only by `ConnTable::reap_if_drained`. Rationale: `quinn`'s
    // internal `Endpoint::handle_event(Drained)` calls
    // `self.connections.try_remove(ch.0)`, releasing quinn's slab slot.
    // `ConnTable.conns` is a strict lockstep mirror of quinn's slab —
    // `get_or_dial`'s `debug_assert_eq!(slot, ch.0)` enforces that the
    // next `connect()` reuses the SAME index in both slabs. Forwarding
    // a connection-emitted `Drained` here would release quinn's slot
    // while our `ConnTable` still holds it; an immediately-following
    // dial then desynchronises the two slabs. `reap_if_drained` is the
    // sole site that pairs `quinn.handle_event(ch,
    // EndpointEvent::drained())` with `self.conns.try_remove(ch.0)`,
    // keeping both slabs in lockstep.
    while let Some(ev) = e.conn_mut().poll_endpoint_events() {
      if ev.is_drained() {
        continue;
      }
      #[cfg(test)]
      {
        self.counters.endpoint_events_processed =
          self.counters.endpoint_events_processed.saturating_add(1);
      }
      if let Some(conn_ev) = self.quinn.handle_event(ch, ev) {
        // Queue for the NEXT iteration of this connection — see
        // [`super::conn::ConnEntry::queue_pending_event`] for the
        // one-tick deferral rationale and the by-construction lifetime
        // co-location that eliminates quinn's `vacant_key()` slab-slot
        // reuse race.
        e.queue_pending_event(conn_ev);
      }
    }
    // Snapshot this connection's deferred-backlog state after the drain-and-
    // requeue above (the only site pending events change). Applied to the
    // O(1) `conns_with_pending_events` index at the end of the iteration so
    // `poll_timeout`'s immediate-due term never scans the connection table.
    let has_pending_events_after = e.has_pending_events();
    // Drain inbound application datagrams into the same mem_ingress the plain-
    // UDP gossip path fills, tagged with this connection's peer. Mode-
    // independent: a Udp-mode endpoint does not negotiate datagrams, so this is
    // a no-op there; a Datagram-mode endpoint extracts them. Pop quinn's buffer
    // to EMPTY (so a zero-length-frame flood cannot accumulate inside quinn),
    // but PUSH into the coordinator queue only while this peer's STANDING share
    // (`mem_ingress_per_peer`, maintained across the whole undrained queue) is
    // under the per-peer budget AND the global cap is not reached — beyond
    // either, drop and count. Bounding the standing share (not a per-pass
    // counter) gives every peer fair access regardless of how many recv passes
    // a driver batches before decoding, so one flooding peer cannot starve
    // another peer's probe Ack. recv() returns an owned Bytes, so the
    // e.conn_mut() borrow releases before the disjoint-field pushes.
    let peer = e.peer();
    while let Some(payload) = e.conn_mut().datagrams().recv() {
      // Pop quinn to empty so a zero-length-frame flood cannot accumulate
      // inside quinn; admission (per-peer + global caps, dropped+counted past
      // either bound so one flooding peer cannot fill the shared queue and
      // starve another peer's probe Ack) is enforced by the shared helper —
      // the SAME bound `handle_memberlist_udp` applies, so neither source can
      // exceed it. The three `&mut self.<field>` args are disjoint from the
      // `self.conns` borrow `e` holds.
      push_mem_ingress_capped(
        &mut self.mem_ingress,
        &mut self.mem_ingress_per_peer,
        &mut self.datagram_ingress_dropped,
        peer,
        move || payload,
      );
    }
    // Also reap when the connection has reached `is_drained()` even if
    // `poll()` never yielded `Event::ConnectionLost` for it in this
    // iteration — the kill-on-idle-timeout path
    // (`kill(ConnectionError::TimedOut)`) and similar immediate-drain
    // transitions set `self.error` and queue
    // `EndpointEventInner::Drained` simultaneously; whether `poll()`
    // surfaced the `ConnectionLost` event THIS tick depends on the
    // internal `events` FIFO ordering. The combined `lost || is_drained()`
    // gate catches both shapes so the strict-poll bridge-leak is closed
    // regardless of which signal arrived first.
    let drained = e.conn_ref().is_drained();
    // Sample the establishment observation AFTER every `conn_mut()` above has had
    // its chance to flip the sticky flag; a `false -> true` across this pass is
    // the establishment mark. Capture the credit-restore peer as a value now so
    // it survives the connection being reaped later in this same pass.
    let established_transition = !established_before && e.established_at_least_once();
    let credit_restored_peer = credit_restored.then_some(peer);
    // `e` borrows `self.conns`; release it before touching `self.bridges`.
    let _ = e;
    if lost || drained {
      // Mark every bridge on this connection fatal AND complete its D1
      // drain_then_reap synchronously, in this same tick.
      //
      // A connection-level loss observed inside `service_quinn` (step 4)
      // runs AFTER `pump_bridges` (step 2) and BEFORE `finalize_tick`
      // (step 5). The freshly-fatal bridges therefore miss this tick's
      // `pump_bridges` D1 reap; a stateless-reset / immediate-drain
      // loss path can then have `finalize_tick` free the connection in
      // the SAME tick — and `Bridge::poll_timeout` returns `None` for
      // terminal bridges (it deliberately owes no future work to
      // itself once terminal). The coordinator's unified `poll_timeout`
      // then has no immediate-due term contributed by these bridges,
      // so a strict-poll driver with no other peer/probe/timer due
      // wakes never again — the terminal bridges leak forever.
      //
      // Coordinator-level fix: when we know a bridge is terminal
      // because of a connection-level event, close out the D1 drain
      // here. `fail_connection_lost()` transitions the bridge phase
      // to `Failed(ConnectionLost)` so the `StreamErrored` lifecycle
      // notice inside `drain_then_reap` carries the connection-loss
      // attribution.
      // Take this connection's WHOLE bucket in one O(1) map remove, then run a
      // bucketless deindex per bridge — no per-bridge SmallVec surgery. Losing a
      // connection holding K bridges (or one datagram FIN-acking K of them)
      // therefore reaps in O(K) total, never O(K²). Bounded by quinn's per-conn
      // stream limit, so an attacker who loses one connection pays only that
      // connection's bridges, never an O(all bridges) scan of the whole table.
      let ids: SmallVec<StreamId> = self.bridges_by_conn.remove(&ch).unwrap_or_default();
      #[cfg(test)]
      {
        self.counters.bridge_scan_visits = self
          .counters
          .bridge_scan_visits
          .saturating_add(ids.len() as u64);
      }
      for id in ids {
        if let Some(mut br) = self.bridges.remove(&id) {
          br.fail_connection_lost();
          br.drain_then_reap(&mut self.ep, &mut self.conns, now);
          #[cfg(test)]
          {
            self.counters.bridges_reaped_on_connection_lost = self
              .counters
              .bridges_reaped_on_connection_lost
              .saturating_add(1);
          }
          // ConnectionLost ⇒ `fail_connection_lost` set the phase to
          // `Failed(ConnectionLost)`; outcome is unconditionally
          // `Failed` here, but route through the shared helper so
          // the kind lookup + emission path matches the
          // `pump_bridges` reap.
          let outcome = Self::outcome_for_terminal(&br);
          let sid = br.sid();
          drop(br);
          // The whole bucket was already removed above, so drop this bridge's
          // remaining index entries (deadline key, `(ch, sid)` reverse lookup,
          // inbound count) WITHOUT any per-bridge bucket surgery — keeping the
          // loss reap O(K) across the connection's bridges.
          self.deindex_reaped_bridge_bucketless(id, ch, sid);
          self.emit_exchange_completed(id, outcome);
        }
      }
    }
    // Release this connection's per-source pending-index unit the pass it
    // establishes. Runs once per connection per servicing pass (after the
    // `conn_mut()` calls above have made the sticky establishment
    // observation), so an inbound connection that completed its handshake
    // this pass leaves the half-open index in the same pass — a no-op for a
    // still-handshaking, outbound, or already-released connection.
    self.conns.reconcile_pending_inbound(ch);
    // Apply the deferred-backlog snapshot to the immediate-due index. A
    // connection reaped later this tick (`finalize_tick`) is removed from the
    // set there, so a stale membership cannot linger.
    if has_pending_events_after {
      self.conns_with_pending_events.insert(ch);
    } else {
      self.conns_with_pending_events.remove(&ch);
    }
    ServiceMarks {
      established_transition,
      credit_restored_peer,
    }
  }
}

#[cfg(test)]
mod tests;
