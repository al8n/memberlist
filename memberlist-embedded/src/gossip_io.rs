//! The gossip-plane datagram I/O abstraction.

use core::net::SocketAddr;

/// Non-blocking UDP datagram I/O for the gossip plane.
///
/// A driver supplies this over its already-ticked UDP socket buffers; the
/// engine reads inbound gossip and writes outbound gossip through it without
/// knowing the underlying stack. Both I/O methods are non-blocking: they move at
/// most one datagram against buffers the driver's stack tick has already
/// filled or drained. [`recv_capacity`](Self::recv_capacity) performs no I/O; it
/// declares the receive ring, from which the engine derives how much of it to
/// read per pump and which it screens against its per-pump work ceiling.
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
  /// It must be the TRUE capacity of the ring `recv` pops from, for the view being
  /// pumped. An engine built on this trait derives each pump's read bound from
  /// this number, so that every datagram the ring held when the pump began is
  /// popped and applied at that pump's instant, before any membership sweep. A
  /// stale or wrong declaration is the one thing no engine can defend against: an
  /// under-declared ring leaves the excess sitting unread — so unobserved and
  /// un-stamped — across the sweep, and a refutation waiting there could be applied
  /// only after the timer it refutes had already fired. Report the ring the engine
  /// will actually read from — the bound socket's own capacity wherever the stack
  /// exposes it — so the declared value cannot drift from the buffer the driver
  /// installed, and re-declare it if the ring is ever resized.
  ///
  /// The engine additionally screens this capacity at construction against its own
  /// per-pump work ceiling and rejects a ring that can hold as many datagrams as
  /// the ceiling or more, which is what bounds the decode work one pump can cost.
  /// See [`GOSSIP_READ_CAP`](crate::GOSSIP_READ_CAP) and
  /// [`Engine::try_new_at`](crate::Engine::try_new_at).
  fn recv_capacity(&self) -> usize;

  /// Best-effort enqueue of one datagram to `dest`. Gossip is lossy: drop on a full tx ring.
  fn send(&mut self, bytes: &[u8], dest: SocketAddr);
}
