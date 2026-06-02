//! The gossip-plane datagram I/O abstraction.

use core::net::SocketAddr;

/// Non-blocking UDP datagram I/O for the gossip plane.
///
/// A driver supplies this over its already-ticked UDP socket buffers; the
/// engine reads inbound gossip and writes outbound gossip through it without
/// knowing the underlying stack. Both methods are non-blocking: they move at
/// most one datagram against buffers the driver's stack tick has already
/// filled or drained.
pub trait GossipIo {
  /// Pop one received datagram into `buf`; `(source, len)` or `None` when the rx ring is empty.
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)>;

  /// Best-effort enqueue of one datagram to `dest`. Gossip is lossy: drop on a full tx ring.
  fn send(&mut self, bytes: &[u8], dest: SocketAddr);
}
