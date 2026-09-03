//! Driver-side sizing and ports for [`Memberlist`](crate::Memberlist).
//!
//! The embassy-net [`Stack`](embassy_net::Stack), its interface, and its IP
//! configuration are owned by the caller (built via [`embassy_net::new`]), so —
//! unlike the smoltcp driver — this config carries no medium / address / route
//! knobs. It holds only what the driver itself needs: the bound port, the bridge
//! ring capacities (which also bound the per-slot mailbox byte rings), the
//! per-socket inactivity timeout, and the engine policy the driver forwards — the
//! graceful-close timeout, the per-pump gossip work ceiling, and the two
//! reliable-dial admission ceilings.

use core::time::Duration;

pub use memberlist_embedded::DEFAULT_CLOSE_TIMEOUT;

/// Ports and bridge sizing for [`Memberlist`](crate::Memberlist).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Options {
  /// Local port the node binds. The gossip UDP socket and the reliable-plane TCP
  /// listener both use it, and it is the port peers reach the node at — the
  /// single-port memberlist model (one advertised `SocketAddr` serves both
  /// planes).
  pub port: u16,
  /// Per-slot inbound (peer → engine) mailbox ring capacity, in bytes. A worker
  /// reads at most this many un-handed-off bytes before the engine drains them.
  /// Defaults to a TCP socket's worth.
  pub tcp_socket_rx_bytes: usize,
  /// Per-slot outbound (engine → peer) mailbox ring capacity, in bytes. The
  /// engine's `send` accepts up to this many un-written bytes per slot. Defaults
  /// to a TCP socket's worth.
  pub tcp_socket_tx_bytes: usize,
  /// Maximum time a gracefully-closing reliable connection may stay parked before
  /// it is force-aborted and returned to the pool (the engine's close bound).
  pub close_timeout: Duration,
  /// Inactivity timeout applied to each reliable-plane TCP socket. It bounds how
  /// long a worker can block in a `connect` / `write` / `flush` / `read` to an
  /// unresponsive peer before embassy-net aborts the socket — so a peer that stops
  /// ACKing cannot wedge the worker (and, via the reuse gate, withhold its slot)
  /// indefinitely. MUST exceed the longest valid reliable exchange AND the
  /// `close_timeout`, or a legitimately slow exchange could be aborted early.
  /// Defaults to a generous multiple of the close timeout.
  pub socket_timeout: Duration,
  /// The engine's per-pump gossip work ceiling, forwarded to
  /// [`memberlist_embedded::Options::gossip_read_cap`].
  ///
  /// The engine rejects a gossip [`UdpSocket`](embassy_net::udp::UdpSocket) whose
  /// receive-packet ring reaches it, and a pump applies every datagram it pops, so
  /// this bounds the unwrap/decode/apply work one wake spends on gossip. The
  /// engine's field docs carry the full contract.
  ///
  /// Must be non-zero. Defaults to [`memberlist_embedded::GOSSIP_READ_CAP`].
  pub gossip_read_cap: usize,
  /// The engine's join-seed queue ceiling, forwarded to
  /// [`memberlist_embedded::Options::max_pending_seeds`].
  ///
  /// [`join`](crate::Memberlist::join) queues each routable seed it is not already
  /// queuing or exchanging state with, and the [`Runner`](crate::Runner) admits them
  /// as the reliable pool has room; this caps how many may WAIT, and a seed offered
  /// past it is dropped rather than failing the call. The engine's field docs carry
  /// the full contract.
  ///
  /// Must be non-zero. Defaults to
  /// [`memberlist_embedded::DEFAULT_MAX_PENDING_SEEDS`].
  pub max_pending_seeds: usize,
  /// The engine's beyond-capacity parked-dial ceiling, forwarded to
  /// [`memberlist_embedded::Options::max_pending_dials`].
  ///
  /// The bound is on the EXCESS — parked dials minus free pool slots — so a burst
  /// with `F` free slots parks the first `F + max_pending_dials` and refuses the
  /// rest, which [`send_reliable`](crate::Memberlist::send_reliable) reports at the
  /// call site as [`OpError::DialBacklogFull`](crate::OpError::DialBacklogFull). The
  /// engine's field docs carry the full contract.
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
      tcp_socket_rx_bytes: 4096,
      tcp_socket_tx_bytes: 4096,
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
      socket_timeout: Duration::from_secs(15),
      gossip_read_cap: memberlist_embedded::GOSSIP_READ_CAP,
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

  /// Override the local port (the gossip UDP socket and the reliable-plane TCP
  /// listener both bind it).
  pub fn with_port(mut self, p: u16) -> Self {
    self.port = p;
    self
  }

  /// Override the per-slot inbound mailbox ring capacity (bytes).
  pub fn with_tcp_socket_rx_bytes(mut self, n: usize) -> Self {
    self.tcp_socket_rx_bytes = n;
    self
  }

  /// Override the per-slot outbound mailbox ring capacity (bytes).
  pub fn with_tcp_socket_tx_bytes(mut self, n: usize) -> Self {
    self.tcp_socket_tx_bytes = n;
    self
  }

  /// Override the graceful-close timeout (see [`Options::close_timeout`]).
  pub fn with_close_timeout(mut self, d: Duration) -> Self {
    self.close_timeout = d;
    self
  }

  /// Override the per-socket inactivity timeout (see [`Options::socket_timeout`]).
  pub fn with_socket_timeout(mut self, d: Duration) -> Self {
    self.socket_timeout = d;
    self
  }

  /// Override the engine's per-pump gossip work ceiling (see
  /// [`Options::gossip_read_cap`]). Must be non-zero.
  pub fn with_gossip_read_cap(mut self, cap: usize) -> Self {
    self.gossip_read_cap = cap;
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
