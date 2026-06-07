//! Transport-agnostic engine sizing: the ports and timeouts the driving core
//! itself depends on.
//!
//! A concrete driver (e.g. [`memberlist-smoltcp`](https://docs.rs/memberlist-smoltcp))
//! carries its OWN link-layer sizing — socket-buffer bytes, UDP arenas, the
//! TCP pool size — on top of this; those govern memory the driver allocates,
//! not protocol behaviour, so they stay on the driver. This struct holds only
//! the knobs the [`Engine`](crate::Engine) reads directly: the bound port (used
//! to `listen` and to derive ephemeral dial ports) and the graceful-close
//! timeout that bounds the reliable plane's drain.

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
  /// Maximum time a gracefully-closing reliable connection may stay parked
  /// before it is force-aborted and returned to the pool.
  ///
  /// A link layer such as smoltcp applies no TCP timeout by default, so a peer
  /// that vanishes during the FIN handshake (FinWait/LastAck) keeps the
  /// connection open indefinitely and its slot never returns to the free-list —
  /// permanently shrinking the pool and the listener replenished from it.
  /// Bounding the close guarantees recovery. A healthy close completes well
  /// before this and is reclaimed the moment it reaches `Closed`; the timeout
  /// only governs the vanished-peer case.
  pub close_timeout: Duration,
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
mod tests {
  use super::*;

  #[test]
  fn defaults_are_sane_and_overridable() {
    let c = Options::new();
    assert_eq!(c.port, 7946);
    assert!(!c.close_timeout.is_zero());
    let c = Options::new()
      .with_port(1234)
      .with_close_timeout(Duration::from_secs(3));
    assert_eq!(c.port, 1234);
    assert_eq!(c.close_timeout, Duration::from_secs(3));
  }
}
