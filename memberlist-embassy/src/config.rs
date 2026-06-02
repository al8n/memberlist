//! Driver-side sizing and ports for [`Memberlist`](crate::Memberlist).
//!
//! The embassy-net [`Stack`](embassy_net::Stack), its interface, and its IP
//! configuration are owned by the caller (built via [`embassy_net::new`]), so —
//! unlike the smoltcp driver — this config carries no medium / address / route
//! knobs. It holds only what the driver itself needs: the bound port, the bridge
//! ring capacities (which also bound the per-slot mailbox byte rings), and the
//! engine's graceful-close timeout.

use core::time::Duration;

pub use memberlist_embedded::DEFAULT_CLOSE_TIMEOUT;

/// Ports and bridge sizing for [`Memberlist`](crate::Memberlist).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
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
}

impl Default for Config {
  fn default() -> Self {
    Self {
      port: 7946,
      tcp_socket_rx_bytes: 4096,
      tcp_socket_tx_bytes: 4096,
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
      socket_timeout: Duration::from_secs(15),
    }
  }
}

impl Config {
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

  /// Override the graceful-close timeout (see [`Config::close_timeout`]).
  pub fn with_close_timeout(mut self, d: Duration) -> Self {
    self.close_timeout = d;
    self
  }

  /// Override the per-socket inactivity timeout (see [`Config::socket_timeout`]).
  pub fn with_socket_timeout(mut self, d: Duration) -> Self {
    self.socket_timeout = d;
    self
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn defaults_are_sane_and_overridable() {
    let c = Config::new();
    assert_eq!(c.port, 7946);
    assert!(c.tcp_socket_rx_bytes > 0);
    assert!(c.tcp_socket_tx_bytes > 0);
    assert!(!c.close_timeout.is_zero());
    assert!(
      c.socket_timeout > c.close_timeout,
      "socket timeout must exceed the close timeout"
    );

    let c = Config::new()
      .with_port(1234)
      .with_tcp_socket_rx_bytes(8192)
      .with_tcp_socket_tx_bytes(2048)
      .with_close_timeout(Duration::from_secs(3))
      .with_socket_timeout(Duration::from_secs(20));
    assert_eq!(c.port, 1234);
    assert_eq!(c.tcp_socket_rx_bytes, 8192);
    assert_eq!(c.tcp_socket_tx_bytes, 2048);
    assert_eq!(c.close_timeout, Duration::from_secs(3));
    assert_eq!(c.socket_timeout, Duration::from_secs(20));
  }
}
