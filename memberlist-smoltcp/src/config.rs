//! Runtime, alloc-backed sizing for the smoltcp driver. The machine is
//! alloc-backed, so fixed-array const generics would buy nothing.

use core::time::Duration;

/// Default [`Config::close_timeout`]: 10 seconds.
///
/// A graceful TCP close (FIN/ACK exchange) over a healthy link completes in a
/// few round-trips; 10 s is a generous bound that rides out WAN latency while
/// still promptly reclaiming a socket whose peer vanished mid-close. It mirrors
/// the machine's default stream/handshake deadline so a stuck reliable exchange
/// and its closing socket are reclaimed on the same order of timescale.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Sizing and ports for [`Memberlist`](crate::Memberlist). All buffers are
/// fixed-capacity at construction (smoltcp has no growable backing on no_std);
/// gossip overflow drops, reliable overflow backpressures.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
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
  pub udp_rx_packets: usize,
  /// UDP tx datagram metadata slots.
  pub udp_tx_packets: usize,
  /// UDP rx payload byte arena.
  pub udp_rx_payload_bytes: usize,
  /// UDP tx payload byte arena.
  pub udp_tx_payload_bytes: usize,
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
}

impl Default for Config {
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
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
    }
  }
}

impl Config {
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

  /// Override the graceful-close timeout (see [`Config::close_timeout`]).
  pub fn with_close_timeout(mut self, d: Duration) -> Self {
    self.close_timeout = d;
    self
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn defaults_are_sane_and_overridable() {
    let c = Config::new();
    assert!(c.tcp_pool_size >= 1);
    assert!(c.udp_rx_payload_bytes > 0);
    let c = Config::new().with_tcp_pool_size(8).with_port(1234);
    assert_eq!(c.tcp_pool_size, 8);
    assert_eq!(c.port, 1234);
  }
}
