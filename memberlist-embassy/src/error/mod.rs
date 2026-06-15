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
  /// ([`Options::tcp_socket_rx_bytes`](crate::Options::tcp_socket_rx_bytes) or
  /// [`tcp_socket_tx_bytes`](crate::Options::tcp_socket_tx_bytes)) is zero.
  ///
  /// A zero-byte inbound ring can never buffer a received byte for the engine to
  /// drain, and a zero-byte outbound ring can never accept a byte from the
  /// engine to write — a silently-dead reliable plane. Both must be non-zero.
  ZeroBridgeRing,
  /// The per-socket inactivity timeout is out of the valid range.
  ///
  /// [`Options::socket_timeout`](crate::Options::socket_timeout), as embassy-net installs it
  /// into smoltcp (floored to whole microseconds — the embassy tick count handed over via
  /// `as_micros`), must be at least one microsecond and strictly greater than BOTH the
  /// graceful-close bound ([`close_timeout`](crate::Options::close_timeout)) and the
  /// machine's reliable-exchange deadline (`EndpointOptions::stream_timeout`) — otherwise
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
  /// The address resolver failed while resolving the advertise address.
  ///
  /// The resolver's error type is generic, so it is boxed to preserve the
  /// `source()` chain; a caller that knows its concrete resolver can downcast.
  /// No `Send`/`Sync` bound — the embassy [`AddressResolver`](crate::AddressResolver)
  /// is single-threaded by design, so its error need not cross threads.
  Resolve(alloc::boxed::Box<dyn core::error::Error + 'static>),
  /// The address resolver succeeded but yielded no address for the advertise
  /// address, so the node would have nothing to advertise.
  NoAddresses,
  /// The platform entropy source ([`getrandom`]) failed while seeding the default
  /// gossip RNG in [`Memberlist::new`](crate::Memberlist::new). Use
  /// [`Memberlist::new_with_rng`](crate::Memberlist::new_with_rng) to supply your
  /// own RNG and avoid the platform entropy draw entirely.
  Entropy,
}

impl InitError {
  /// Whether construction failed because the TCP socket pool had fewer than two
  /// sockets.
  #[inline]
  pub const fn is_tcp_pool_too_small(&self) -> bool {
    matches!(self, InitError::TcpPoolTooSmall(_))
  }

  /// Whether a configured bridge ring capacity was zero.
  #[inline]
  pub const fn is_zero_bridge_ring(&self) -> bool {
    matches!(self, InitError::ZeroBridgeRing)
  }

  /// Whether the per-socket inactivity timeout was out of range.
  #[inline]
  pub const fn is_socket_timeout_out_of_range(&self) -> bool {
    matches!(self, InitError::SocketTimeoutOutOfRange(_))
  }

  /// Whether the shared engine rejected the configuration.
  #[inline]
  pub const fn is_engine(&self) -> bool {
    matches!(self, InitError::Engine(_))
  }

  /// Whether the resolver failed on the advertise address.
  #[inline]
  pub const fn is_resolve(&self) -> bool {
    matches!(self, InitError::Resolve(_))
  }

  /// Whether the resolver yielded no address for the advertise address.
  #[inline]
  pub const fn is_no_addresses(&self) -> bool {
    matches!(self, InitError::NoAddresses)
  }

  /// Whether the platform entropy source failed while seeding the gossip RNG.
  #[inline]
  pub const fn is_entropy(&self) -> bool {
    matches!(self, InitError::Entropy)
  }
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
      InitError::Resolve(e) => write!(f, "advertise address resolution failed: {e}"),
      InitError::NoAddresses => f.write_str("advertise address resolution returned no addresses"),
      InitError::Entropy => f.write_str("entropy source failed while seeding the gossip RNG"),
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
      InitError::Resolve(e) => Some(e.as_ref()),
      _ => None,
    }
  }
}

/// Why an awaiting handle operation ([`Memberlist::ping`](crate::Memberlist::ping)
/// / [`send_reliable`](crate::Memberlist::send_reliable) /
/// [`join`](crate::Memberlist::join)) did not succeed.
///
/// `Resolve` is `!Clone` / `!PartialEq` (it boxes the resolver's generic error),
/// so this enum derives neither — match on the variant (or the `is_*`
/// predicates) instead.
#[derive(Debug)]
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
  /// The address resolver failed while resolving a [`join`](crate::Memberlist::join)
  /// seed.
  ///
  /// The resolver's error type is generic, so it is boxed to preserve the
  /// `source()` chain; a caller that knows its concrete resolver can downcast.
  /// No `Send`/`Sync` bound — the embassy [`AddressResolver`](crate::AddressResolver)
  /// is single-threaded by design, so its error need not cross threads.
  Resolve(alloc::boxed::Box<dyn core::error::Error + 'static>),
  /// A non-empty [`join`](crate::Memberlist::join) seed set resolved to no wire
  /// address — a discovery failure rather than a successful no-op join.
  NoAddresses,
}

impl OpError {
  /// Whether a ping timed out without an ack.
  #[inline]
  pub const fn is_ping_timeout(&self) -> bool {
    matches!(self, OpError::PingTimeout)
  }

  /// Whether a reliable send terminated without success.
  #[inline]
  pub const fn is_send_failed(&self) -> bool {
    matches!(self, OpError::SendFailed)
  }

  /// Whether the operation was rejected because the node is not running.
  #[inline]
  pub const fn is_not_running(&self) -> bool {
    matches!(self, OpError::NotRunning)
  }

  /// Whether the resolver failed on a join seed.
  #[inline]
  pub const fn is_resolve(&self) -> bool {
    matches!(self, OpError::Resolve(_))
  }

  /// Whether a non-empty join seed set resolved to no wire address.
  #[inline]
  pub const fn is_no_addresses(&self) -> bool {
    matches!(self, OpError::NoAddresses)
  }
}

impl fmt::Display for OpError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      OpError::PingTimeout => f.write_str("ping timed out: no ack within the probe timeout"),
      OpError::SendFailed => f.write_str("reliable send failed"),
      OpError::NotRunning => f.write_str("node is not in a running state"),
      OpError::Resolve(e) => write!(f, "seed address resolution failed: {e}"),
      OpError::NoAddresses => f.write_str("no wire address resolved for any seed"),
    }
  }
}

#[cfg(feature = "std")]
impl std::error::Error for OpError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      OpError::Resolve(e) => Some(e.as_ref()),
      _ => None,
    }
  }
}

#[cfg(all(test, feature = "std"))]
mod tests;
