//! Runtime, alloc-backed sizing for the smoltcp driver. The machine is
//! alloc-backed, so fixed-array const generics would buy nothing.

use core::time::Duration;

/// Default [`Options::close_timeout`]: 10 seconds.
///
/// A graceful TCP close (FIN/ACK exchange) over a healthy link completes in a
/// few round-trips; 10 s is a generous bound that rides out WAN latency while
/// still promptly reclaiming a socket whose peer vanished mid-close. It mirrors
/// the machine's default stream/handshake deadline so a stuck reliable exchange
/// and its closing socket are reclaimed on the same order of timescale.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default [`Options::ingress_packets_per_poll`]: 16 device packets.
///
/// Twice the default `udp_rx_packets`, so a burst that fills the gossip ring
/// still leaves per-poll budget for the reliable plane's segments, while a
/// sustained flood is cut off after a bounded amount of stack work and the engine
/// runs on every call.
pub const DEFAULT_INGRESS_PACKETS_PER_POLL: usize = 16;

/// Sizing and ports for [`Memberlist`](crate::Memberlist). All buffers are
/// fixed-capacity at construction (smoltcp has no growable backing on no_std);
/// gossip overflow drops, reliable overflow backpressures.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
  /// Local port the node binds. The gossip UDP socket and the reliable-plane
  /// TCP listener both use it, and it is the port peers reach the node at — the
  /// single-port memberlist model (one advertised `SocketAddr` serves both
  /// planes, since a UDP and a TCP socket on the same port number are
  /// independent).
  pub port: u16,
  /// Pooled TCP sockets (max concurrent reliable exchanges + 1 listener).
  pub tcp_pool_size: usize,
  /// Per-TCP-socket rx ring bytes.
  pub tcp_socket_rx_bytes: usize,
  /// Per-TCP-socket tx ring bytes.
  pub tcp_socket_tx_bytes: usize,
  /// UDP rx datagram metadata slots.
  ///
  /// Must be strictly below [`gossip_read_cap`](Self::gossip_read_cap), which
  /// construction enforces — here, before the UDP arenas this count sizes are
  /// allocated, and again in the engine against the bound socket. The engine
  /// applies every datagram it pops within that pump, so this count is the
  /// per-pump gossip work budget.
  pub udp_rx_packets: usize,
  /// UDP tx datagram metadata slots.
  pub udp_tx_packets: usize,
  /// UDP rx payload byte arena.
  pub udp_rx_payload_bytes: usize,
  /// UDP tx payload byte arena.
  pub udp_tx_payload_bytes: usize,
  /// The engine's per-pump gossip work ceiling, forwarded to
  /// [`memberlist_embedded::Options::gossip_read_cap`].
  ///
  /// Construction rejects a [`udp_rx_packets`](Self::udp_rx_packets) at or above
  /// it (before the UDP arenas are allocated), and the engine rejects a bound
  /// gossip socket whose receive ring reaches it. A pump applies every datagram it
  /// pops, so this bounds the unwrap/decode/apply work one
  /// [`poll`](crate::Memberlist::poll) spends on gossip; its byte implication is
  /// `gossip_read_cap × (gossip_mtu + transform overhead)`.
  ///
  /// Must be non-zero. Defaults to [`memberlist_embedded::GOSSIP_READ_CAP`].
  pub gossip_read_cap: usize,
  /// Maximum device packets one [`poll`](crate::Memberlist::poll) feeds into the
  /// smoltcp stack before it runs the engine.
  ///
  /// smoltcp's own `Interface::poll` drains the device until it stops yielding,
  /// which is unbounded work when packets arrive faster than they are processed —
  /// its documented DoS caveat. A caller-driven super-loop has no preemption, so
  /// an unbounded ingress phase would starve every SWIM timer, the application's
  /// event drain, and whatever else shares the loop. `poll` therefore feeds at
  /// most this many packets per call and always reaches the engine; when the
  /// budget is spent with the device still yielding, `poll` returns an
  /// already-due deadline meaning "device backlog remains: service your other
  /// work, then poll again".
  ///
  /// This is a device-fairness knob, INDEPENDENT of the gossip ring: a gossip
  /// datagram arriving with no free slot in [`udp_rx_packets`](Self::udp_rx_packets)
  /// is tail-dropped by smoltcp's UDP socket inside the ingress loop whatever this
  /// value is (`udp::Socket::process` drops on a full rx buffer). So raising it
  /// buys TCP and device progress per poll, not gossip intake — raise
  /// `udp_rx_packets` (below [`gossip_read_cap`](Self::gossip_read_cap)) for that.
  ///
  /// The tradeoff is throughput against latency: a larger budget clears more of a
  /// device backlog per call and amortises the per-poll engine work, while a
  /// smaller one returns to the caller — and to the SWIM timers — sooner under
  /// sustained ingress. Must be non-zero; the default of 16 is twice the shipped
  /// `udp_rx_packets`, so an idle-to-busy burst that fills the gossip ring still
  /// leaves budget for the reliable plane's segments in the same poll.
  pub ingress_packets_per_poll: usize,
  /// Maximum time a gracefully-closing TCP socket may stay parked before it is
  /// force-aborted and returned to the pool.
  ///
  /// smoltcp applies no TCP timeout by default, so a peer that vanishes during
  /// the FIN handshake (FinWait/LastAck) keeps the socket open indefinitely and
  /// the handle never returns to the free-list — permanently shrinking the pool
  /// and the listener replenished from it. Bounding the close guarantees
  /// recovery. A healthy close completes well before this and is reclaimed the
  /// moment it reaches `Closed`; the timeout only governs the vanished-peer case.
  pub close_timeout: Duration,
  /// The engine's join-seed queue ceiling, forwarded to
  /// [`memberlist_embedded::Options::max_pending_seeds`].
  ///
  /// [`join`](crate::Memberlist::join) queues each routable seed it is not already
  /// queuing or exchanging state with, and a [`poll`](crate::Memberlist::poll)
  /// admits them as the TCP pool has room; this caps how many may WAIT, and a seed
  /// offered past it is dropped rather than failing the call. The engine's field
  /// docs carry the full contract.
  ///
  /// Must be non-zero. Defaults to
  /// [`memberlist_embedded::DEFAULT_MAX_PENDING_SEEDS`].
  pub max_pending_seeds: usize,
  /// The engine's beyond-capacity parked-dial ceiling, forwarded to
  /// [`memberlist_embedded::Options::max_pending_dials`].
  ///
  /// The bound is on the EXCESS — parked dials minus free pool sockets — so a burst
  /// with `F` free sockets parks the first `F + max_pending_dials` and refuses the
  /// rest, which [`send_reliable`](crate::Memberlist::send_reliable) reports at the
  /// call site as backpressure. The engine's field docs carry the full contract.
  ///
  /// Must be non-zero. Defaults to
  /// [`memberlist_embedded::DEFAULT_MAX_PENDING_DIALS`].
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
      tcp_pool_size: 4,
      tcp_socket_rx_bytes: 4096,
      tcp_socket_tx_bytes: 4096,
      udp_rx_packets: 8,
      udp_tx_packets: 8,
      udp_rx_payload_bytes: 8 * 1500,
      udp_tx_payload_bytes: 8 * 1500,
      gossip_read_cap: memberlist_embedded::GOSSIP_READ_CAP,
      ingress_packets_per_poll: DEFAULT_INGRESS_PACKETS_PER_POLL,
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
      max_pending_seeds: memberlist_embedded::DEFAULT_MAX_PENDING_SEEDS,
      max_pending_dials: memberlist_embedded::DEFAULT_MAX_PENDING_DIALS,
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

  /// Override the pooled TCP socket count.
  pub fn with_tcp_pool_size(mut self, n: usize) -> Self {
    self.tcp_pool_size = n;
    self
  }

  /// Override the local port (the gossip UDP socket and the reliable-plane TCP
  /// listener both bind it).
  pub fn with_port(mut self, p: u16) -> Self {
    self.port = p;
    self
  }

  /// Override the graceful-close timeout (see [`Options::close_timeout`]).
  pub fn with_close_timeout(mut self, d: Duration) -> Self {
    self.close_timeout = d;
    self
  }

  /// Override the engine's per-pump gossip work ceiling (see
  /// [`Options::gossip_read_cap`]). Must be non-zero.
  pub fn with_gossip_read_cap(mut self, cap: usize) -> Self {
    self.gossip_read_cap = cap;
    self
  }

  /// Override the per-poll device ingress budget (see
  /// [`Options::ingress_packets_per_poll`]). Must be non-zero.
  pub fn with_ingress_packets_per_poll(mut self, n: usize) -> Self {
    self.ingress_packets_per_poll = n;
    self
  }

  /// Override the engine's join-seed queue ceiling (see
  /// [`Options::max_pending_seeds`]). Must be non-zero.
  pub fn with_max_pending_seeds(mut self, cap: usize) -> Self {
    self.max_pending_seeds = cap;
    self
  }

  /// Override the engine's beyond-capacity parked-dial ceiling (see
  /// [`Options::max_pending_dials`]). Must be non-zero.
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
