//! Transport-agnostic engine sizing: the ports and timeouts the driving core
//! itself depends on.
//!
//! A concrete driver (e.g. [`memberlist-smoltcp`](https://docs.rs/memberlist-smoltcp))
//! carries its OWN link-layer sizing — socket-buffer bytes, UDP arenas, the
//! TCP pool size — on top of this; those govern memory the driver allocates,
//! not protocol behaviour, so they stay on the driver. This struct holds only
//! the knobs the [`Engine`](crate::Engine) reads directly: the bound port (used
//! to `listen` and to derive ephemeral dial ports), the graceful-close timeout
//! that bounds the reliable plane's drain, and the per-pump gossip work ceiling
//! that bounds which gossip receive rings the engine accepts.

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
