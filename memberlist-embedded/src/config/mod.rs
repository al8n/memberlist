//! Transport-agnostic engine sizing: the ports and timeouts the driving core
//! itself depends on.
//!
//! A concrete driver (e.g. [`memberlist-smoltcp`](https://docs.rs/memberlist-smoltcp))
//! carries its OWN link-layer sizing — socket-buffer bytes, UDP arenas, the
//! TCP pool size — on top of this; those govern memory the driver allocates,
//! not protocol behaviour, so they stay on the driver. This struct holds only
//! the knobs the [`Engine`](crate::Engine) reads directly: the bound port (used
//! to `listen` and to derive ephemeral dial ports), the graceful-close timeout
//! that bounds the reliable plane's drain, the per-pump gossip work ceiling that
//! bounds which gossip receive rings the engine accepts, and the two admission
//! bounds that cap how much reliable dial intent a caller can accumulate.

use core::time::Duration;

/// Default [`Options::close_timeout`]: 10 seconds.
///
/// A graceful TCP close (FIN/ACK exchange) over a healthy link completes in a
/// few round-trips; 10 s is a generous bound that rides out WAN latency while
/// still promptly reclaiming a connection whose peer vanished mid-close. It
/// mirrors the machine's default stream/handshake deadline so a stuck reliable
/// exchange and its closing connection are reclaimed on the same order of
/// timescale.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default [`Options::max_pending_seeds`]: 32 seed addresses.
///
/// One `join` call may resolve four hostnames into
/// [`MAX_RESOLVED_ADDRS_PER_SEED`](crate::MAX_RESOLVED_ADDRS_PER_SEED) addresses
/// each, so 32 admits a full multi-name seed list without truncation while still
/// bounding what a repeated caller can accumulate. A queued seed costs only its
/// address until the pump admits it, so the ceiling is on intent, not memory.
pub const DEFAULT_MAX_PENDING_SEEDS: usize = 32;

/// Default [`Options::max_pending_dials`]: 8 dials waiting on a pool that could
/// back none of them.
///
/// Twice the reliable pool a small embedded node typically runs, so a burst is
/// absorbed rather than truncated, while a runaway caller is stopped well before
/// the parked requests dominate memory: each parked dial can hold up to one
/// `max_stream_frame_size` of request bytes plus its machine-side bridge.
pub const DEFAULT_MAX_PENDING_DIALS: usize = 8;

/// Ports and timeouts for the [`Engine`](crate::Engine).
///
/// All values are policy the driving core reads directly; link-layer buffer
/// sizing lives on the concrete driver. [`Default`] binds the IANA memberlist
/// port (7946) with the [`DEFAULT_CLOSE_TIMEOUT`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
  /// Local port the node binds. The gossip plane and the reliable-plane
  /// listener both use it, and it is the port peers reach the node at — the
  /// single-port memberlist model (one advertised `SocketAddr` serves both
  /// planes, since a datagram and a stream socket on the same port number are
  /// independent).
  pub port: u16,
  /// Maximum time EACH PHASE of a reliable connection's teardown may stay parked
  /// before it is force-aborted and its slot returned to the pool.
  ///
  /// It is a HARD CAP applied per phase — the pre-FIN drain (`Closing`), the FIN
  /// handshake (the `Draining` retire), and the RST egress (the `Aborting`
  /// retire) — and is set once per phase, never re-armed by peer progress. So a
  /// graceful teardown completes or is force-aborted within at most two windows
  /// (plus one more for the RST egress), and NO peer behaviour — including an
  /// indefinitely-slow but progressing ACK trickle — can extend any window.
  ///
  /// A link layer such as smoltcp applies no TCP timeout by default, so a peer
  /// that vanishes during the FIN handshake (FinWait/LastAck) — or one that keeps
  /// trickling ACKs to defer the pre-FIN drain forever — would otherwise keep the
  /// connection open indefinitely and its slot never returns to the free-list,
  /// permanently shrinking the pool and the listener replenished from it. The hard
  /// cap guarantees recovery. A healthy close completes well before this and is
  /// reclaimed the moment its teardown is acknowledged; the timeout only governs
  /// the vanished-, stalled-, or trickling-peer case.
  pub close_timeout: Duration,
  /// The per-pump gossip work ceiling: the largest gossip receive ring the
  /// [`Engine`](crate::Engine) accepts at construction.
  ///
  /// This is a CONSTRUCTION-TIME policy, not the read loop's bound.
  /// [`Engine::try_new_at`](crate::Engine::try_new_at) rejects a
  /// [`GossipIo`](crate::GossipIo) whose declared
  /// [`recv_capacity`](crate::GossipIo::recv_capacity) is at or above this value;
  /// a pump then reads the ring the view in hand declares, so on an integration
  /// that pumps the view it constructed with, per-pump unwrap/decode/apply work is
  /// bounded by this count.
  ///
  /// The bound is a count, not a byte budget. Its byte implication is
  /// `gossip_read_cap × gossip_recv_buf_size(gossip_mtu)`: per-pump AEAD and
  /// decode cost scales with bytes, so the count is a CPU ceiling only at the
  /// configured MTU. Raising it raises both.
  ///
  /// Must be non-zero ([`InitError::ZeroGossipReadCap`](crate::InitError::ZeroGossipReadCap));
  /// [`GOSSIP_READ_CAP`](crate::GOSSIP_READ_CAP) is the default.
  pub gossip_read_cap: usize,
  /// Largest number of join seed addresses that may wait in the engine's queue
  /// at once.
  ///
  /// [`Engine::join`](crate::Engine::join) queues each routable seed it has not
  /// already queued and has no live join exchange to; the pump then admits them a
  /// few at a time, as the reliable pool has room. This caps how many may WAIT. A
  /// seed offered past the cap is dropped and counted
  /// ([`Engine::join_seeds_dropped`](crate::Engine::join_seeds_dropped)) rather
  /// than rejecting the call: a seed list is best-effort discovery intent, and the
  /// engine already drops non-routable seeds silently. WHICH seeds a full queue
  /// sheds follows a stable per-address order rather than the caller's, so
  /// repeated offers of an over-cap list reach every entry in it round-robin (see
  /// [`Engine::join`](crate::Engine::join)).
  ///
  /// A queued seed waits behind at most the dials already outstanding when the pump
  /// admitted it, never behind ones requested later: alongside the seeds the pool
  /// can immediately back, the pump admits the queue's HEAD as a real exchange, so
  /// it holds a place in the machine's dial order ahead of every later request.
  /// That is an ordering guarantee, not a delivery one — the head owns an exchange
  /// deadline from admission and can expire while it waits, which the usual
  /// retry-until-joined loop covers by re-offering it.
  ///
  /// The cap only bites on more than this many DISTINCT routable seeds queued at
  /// once. A small embedded pool could not service that many inside any sane join
  /// deadline anyway — each unreachable seed occupies a slot for a full
  /// `stream_timeout`.
  ///
  /// Must be non-zero ([`InitError::ZeroMaxPendingSeeds`](crate::InitError::ZeroMaxPendingSeeds));
  /// [`DEFAULT_MAX_PENDING_SEEDS`] is the default.
  pub max_pending_seeds: usize,
  /// Largest number of caller- and protocol-originated reliable dials that may stay
  /// parked on a pool that could back none of them.
  ///
  /// A HARD post-pump ceiling: after any [`pump`](crate::Engine::pump) at most this
  /// many such dials are parked. It is enforced last among the reliable phases, once
  /// the pump's dial site has spent every free slot — on the listener first, then on
  /// the oldest parked exchanges — so no free slot is ever left idle by the cap, and
  /// what the cap then measures is exactly the intent the pool could not back. Each
  /// waiting dial holds its request bytes (up to one `max_stream_frame_size`) and a
  /// machine-side bridge until it is dialed or fails, so that unbacked remainder is
  /// what actually costs memory. The NEWEST intent is shed first, leaving the oldest.
  ///
  /// It is enforced after the action drain as well, so the parked set it measures is
  /// the one that survived this tick's teardowns rather than one still holding
  /// entries the same drain is about to remove.
  ///
  /// Join seeds stand OUTSIDE the ceiling — neither shed by it nor counted against
  /// it. The engine admits each against measured pool capacity, plus at most one
  /// queue head waiting past that capacity to hold the seed queue's place in the
  /// dial order. So the total parked population can stand above this ceiling by the
  /// engine's own join admissions, bounded by the pool size plus one; what a caller
  /// can put there is bounded by the ceiling itself.
  ///
  /// [`Engine::send_reliable`](crate::Engine::send_reliable) reports the ceiling at
  /// the call site as `UserDialBacklogFull` — backpressure, not a delivery failure —
  /// so an application can pace itself. That check measures the backlog against the
  /// slots free right now, which the pump will spend on it before anything new can
  /// park, so a send admitted there can still be shed by the ceiling if those slots
  /// went to the listener or to an older seed head. Any other dial source (the
  /// periodic push/pull, a reliable-ping fallback) is failed through the machine's
  /// own never-connected path instead and counted by
  /// [`Engine::pending_dial_rejections`](crate::Engine::pending_dial_rejections);
  /// both retry on their own schedule.
  ///
  /// Must be non-zero ([`InitError::ZeroMaxPendingDials`](crate::InitError::ZeroMaxPendingDials));
  /// [`DEFAULT_MAX_PENDING_DIALS`] is the default.
  pub max_pending_dials: usize,
  /// CIDR peer-admission policy. Filters inbound gossip by datagram source and
  /// inbound reliable connections by peer address at the transport boundary, AND
  /// inbound alives by the peer's self-advertised address at membership
  /// admission. `None` (the default) admits every address. Present only with the
  /// `cidr` feature; set it via [`with_cidr_policy`](Options::with_cidr_policy).
  #[cfg(feature = "cidr")]
  #[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
  pub cidr_policy: Option<memberlist_proto::CidrPolicy>,
}

impl Default for Options {
  fn default() -> Self {
    Self {
      port: 7946,
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
      gossip_read_cap: crate::GOSSIP_READ_CAP,
      max_pending_seeds: DEFAULT_MAX_PENDING_SEEDS,
      max_pending_dials: DEFAULT_MAX_PENDING_DIALS,
      #[cfg(feature = "cidr")]
      cidr_policy: None,
    }
  }
}

impl Options {
  /// Defaults tuned for a small embedded cluster.
  pub fn new() -> Self {
    Self::default()
  }

  /// Override the local port (the gossip plane and the reliable-plane listener
  /// both bind it).
  pub fn with_port(mut self, p: u16) -> Self {
    self.port = p;
    self
  }

  /// Override the graceful-close timeout (see [`Options::close_timeout`]).
  pub fn with_close_timeout(mut self, d: Duration) -> Self {
    self.close_timeout = d;
    self
  }

  /// Override the per-pump gossip work ceiling (see
  /// [`Options::gossip_read_cap`]). Must be non-zero.
  pub fn with_gossip_read_cap(mut self, cap: usize) -> Self {
    self.gossip_read_cap = cap;
    self
  }

  /// Override the join-seed queue ceiling (see [`Options::max_pending_seeds`]).
  /// Must be non-zero.
  pub fn with_max_pending_seeds(mut self, cap: usize) -> Self {
    self.max_pending_seeds = cap;
    self
  }

  /// Override the parked-dial ceiling (see [`Options::max_pending_dials`]). Must be
  /// non-zero.
  pub fn with_max_pending_dials(mut self, cap: usize) -> Self {
    self.max_pending_dials = cap;
    self
  }

  /// Install a CIDR peer-admission policy (see [`Options::cidr_policy`]). One
  /// policy gates the gossip source and reliable peer at the transport boundary
  /// AND the advertised address at membership admission.
  #[cfg(feature = "cidr")]
  #[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
  pub fn with_cidr_policy(mut self, policy: memberlist_proto::CidrPolicy) -> Self {
    self.cidr_policy = Some(policy);
    self
  }
}

#[cfg(test)]
mod tests;
