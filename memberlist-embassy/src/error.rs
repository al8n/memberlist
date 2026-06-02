//! Errors from constructing a [`Memberlist`](crate::Memberlist).

use core::{fmt, time::Duration};

/// Why constructing a [`Memberlist`](crate::Memberlist) node failed.
///
/// Layers a few embassy-driver construction faults (the TCP socket pool size and
/// the bridge ring capacities) on top of the transport-agnostic
/// [`memberlist_embedded::InitError`] the shared engine surfaces (port, advertise
/// address, gossip MTU, encryption-keyring, machine-endpoint faults). Every
/// variant is a misconfiguration reported in place of a panic.
#[derive(Debug)]
#[non_exhaustive]
pub enum InitError {
  /// Fewer than two TCP sockets were supplied.
  ///
  /// Construction dedicates one pooled socket to the listener and uses the rest
  /// for dials/accepts: zero sockets is no reliable plane at all, and one leaves
  /// the listener holding the only socket with none free to dial — the node
  /// could never dial a seed to join. The functional minimum is a listener plus
  /// one dial/accept socket. The supplied count is carried for diagnostics.
  TcpPoolTooSmall(usize),
  /// A configured bridge ring capacity
  /// ([`Config::tcp_socket_rx_bytes`](crate::Config::tcp_socket_rx_bytes) or
  /// [`tcp_socket_tx_bytes`](crate::Config::tcp_socket_tx_bytes)) is zero.
  ///
  /// A zero-byte inbound ring can never buffer a received byte for the engine to
  /// drain, and a zero-byte outbound ring can never accept a byte from the
  /// engine to write — a silently-dead reliable plane. Both must be non-zero.
  ZeroBridgeRing,
  /// The per-socket inactivity timeout is out of the valid range.
  ///
  /// [`Config::socket_timeout`](crate::Config::socket_timeout), as embassy-net installs it
  /// into smoltcp (floored to whole microseconds — the embassy tick count handed over via
  /// `as_micros`), must be at least one microsecond and strictly greater than BOTH the
  /// graceful-close bound ([`close_timeout`](crate::Config::close_timeout)) and the
  /// machine's reliable-exchange deadline (`EndpointConfig::stream_timeout`) — otherwise
  /// embassy-net could abort a slow-but-valid exchange before the engine's own policy
  /// fires — AND no larger than a sane maximum, so it cannot overflow the
  /// embassy-time-to-smoltcp duration conversion into a wrapped (effectively past)
  /// deadline. Because the bound is enforced on the value rounded DOWN to whole installed
  /// microseconds, a coarse or very fine tick rate can reject a timeout that looks valid as
  /// a `core::Duration`; the offending values, the maximum, and the platform tick rate are
  /// carried for diagnostics.
  SocketTimeoutOutOfRange(SocketTimeoutOutOfRange),
  /// The shared engine rejected the configuration (see
  /// [`memberlist_embedded::InitError`]): a zero/over-ceiling gossip MTU, a
  /// non-routable or port-mismatched advertise address, a zero port or
  /// close-timeout, an unusable encryption keyring, or a machine-endpoint init
  /// failure (including an entropy draw failure).
  Engine(memberlist_embedded::InitError),
}

/// Payload for [`InitError::SocketTimeoutOutOfRange`]: the configured socket timeout,
/// the two engine deadlines it must exceed, the maximum it must not exceed, and the
/// platform tick rate the rounded comparison used (a coarse rate can reject a timeout
/// that looks valid before it is rounded down to whole ticks).
#[derive(Debug, Clone, Copy)]
pub struct SocketTimeoutOutOfRange {
  /// The configured per-socket inactivity timeout.
  pub socket_timeout: Duration,
  /// The graceful-close bound it must exceed (after flooring to installed microseconds).
  pub close_timeout: Duration,
  /// The machine's reliable-exchange deadline it must exceed (after flooring to installed
  /// microseconds).
  pub stream_timeout: Duration,
  /// The maximum it must not exceed, so its conversion cannot overflow.
  pub max: Duration,
  /// The platform `embassy-time` tick rate (Hz) that determines the installed-microsecond
  /// flooring the comparison used.
  pub tick_hz: u64,
}

impl fmt::Display for InitError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      InitError::TcpPoolTooSmall(n) => write!(
        f,
        "the TCP socket pool needs at least 2 sockets (a listener plus one \
         dial/accept socket); got {n}"
      ),
      InitError::ZeroBridgeRing => {
        f.write_str("tcp_socket_rx_bytes and tcp_socket_tx_bytes must both be non-zero")
      }
      InitError::SocketTimeoutOutOfRange(s) => write!(
        f,
        "socket_timeout ({:?}), as installed into smoltcp (floored to whole microseconds \
         at the {} Hz platform tick rate), must be at least one microsecond and greater \
         than both close_timeout ({:?}) and stream_timeout ({:?}), and no larger than {:?}",
        s.socket_timeout, s.tick_hz, s.close_timeout, s.stream_timeout, s.max
      ),
      InitError::Engine(e) => write!(f, "{e}"),
    }
  }
}

impl From<memberlist_embedded::InitError> for InitError {
  fn from(e: memberlist_embedded::InitError) -> Self {
    InitError::Engine(e)
  }
}

#[cfg(feature = "std")]
impl std::error::Error for InitError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      InitError::Engine(e) => Some(e),
      _ => None,
    }
  }
}

/// Why an awaiting handle operation ([`Memberlist::ping`](crate::Memberlist::ping)
/// / [`send_reliable`](crate::Memberlist::send_reliable)) did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpError {
  /// The peer did not acknowledge a [`ping`](crate::Memberlist::ping) within the
  /// probe timeout (the machine emitted `Event::PingFailed`).
  PingTimeout,
  /// A reliable exchange ([`send_reliable`](crate::Memberlist::send_reliable))
  /// terminated without success (dial failure, decode/record fault, or deadline).
  SendFailed,
  /// The machine rejected the operation because the node is not in a running
  /// state (e.g. it has already left the cluster).
  NotRunning,
}

impl fmt::Display for OpError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      OpError::PingTimeout => f.write_str("ping timed out: no ack within the probe timeout"),
      OpError::SendFailed => f.write_str("reliable send failed"),
      OpError::NotRunning => f.write_str("node is not in a running state"),
    }
  }
}

#[cfg(feature = "std")]
impl std::error::Error for OpError {}
