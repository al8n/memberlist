//! The gossip-plane datagram I/O abstraction.

use core::net::SocketAddr;

/// Non-blocking UDP datagram I/O for the gossip plane.
///
/// A driver supplies this over its already-ticked UDP socket buffers; the
/// engine reads inbound gossip and writes outbound gossip through it without
/// knowing the underlying stack. Both I/O methods are non-blocking: they move at
/// most one datagram against buffers the driver's stack tick has already
/// filled or drained. [`recv_capacity`](Self::recv_capacity) performs no I/O; it
/// declares the receive ring the engine validates against its per-pump read cap.
pub trait GossipIo {
  /// Pop one received datagram into `buf`; `(source, len)` or `None` when the rx ring is empty.
  ///
  /// The ring must be FIFO: an engine reads it in arrival order and stamps each
  /// datagram with the instant it was popped, so a reordering ring would hand the
  /// machine evidence out of the order the wire delivered it.
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)>;

  /// The maximum number of datagrams the receive ring can hold between two engine
  /// pumps.
  ///
  /// This is the ring's capacity, not its current occupancy — the number of
  /// datagrams the link layer could leave waiting for [`recv`](Self::recv) while
  /// the engine is away.
  ///
  /// An engine built on this trait reads a bounded number of datagrams per pump
  /// and applies every one of them at that pump's instant. It therefore validates
  /// this capacity against its own per-pump read cap when it is constructed and
  /// rejects a ring that can hold as many datagrams as the cap or more: the excess
  /// would sit in the ring unread — so unobserved and un-stamped — across a pump's
  /// membership sweep, and a refutation waiting there could be applied only after
  /// the timer it refutes had already fired. See
  /// [`GOSSIP_READ_CAP`](crate::GOSSIP_READ_CAP) and
  /// [`Engine::try_new_at`](crate::Engine::try_new_at).
  ///
  /// Report the ring the engine will actually read from — the bound socket's own
  /// capacity wherever the stack exposes it — so the declared value cannot drift
  /// from the buffer the driver installed.
  fn recv_capacity(&self) -> usize;

  /// Best-effort enqueue of one datagram to `dest`. Gossip is lossy: drop on a full tx ring.
  fn send(&mut self, bytes: &[u8], dest: SocketAddr);
}
