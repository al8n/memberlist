//! The [`GossipIo`](memberlist_embedded::GossipIo) implementation over the bound
//! smoltcp gossip `udp::Socket`.
//!
//! A short-lived view, rebuilt each [`Memberlist::poll`](crate::Memberlist::poll)
//! over the already-ticked socket set and the gossip UDP [`SocketHandle`]. The
//! engine reads inbound gossip and writes outbound gossip through it without
//! touching smoltcp; the actual stack tick (`iface.poll`) the driver performs
//! before handing this view to the engine.
//!
//! smoltcp keeps every socket — the gossip UDP socket and the reliable-plane TCP
//! pool alike — in one [`SocketSet`], so the gossip and stream views must share
//! mutable access to it. They borrow it through a [`RefCell`] the driver holds for
//! the duration of one pump; each trait method takes a brief `borrow_mut`, never
//! holding a socket borrow across a call into the other view, so the borrows never
//! overlap at runtime.

use core::{cell::RefCell, net::SocketAddr};

use memberlist_embedded::GossipIo;
use smoltcp::{
  iface::{SocketHandle, SocketSet},
  socket::udp,
};

use crate::addr::{from_endpoint, to_endpoint};

/// A [`GossipIo`] view over the gossip `udp::Socket` in a shared [`SocketSet`].
///
/// Resolves the gossip socket by its [`SocketHandle`] on each call, taking a brief
/// `borrow_mut` of the shared set.
pub(crate) struct SmoltcpGossip<'a, 'b> {
  sockets: &'a RefCell<&'a mut SocketSet<'b>>,
  udp: SocketHandle,
}

impl<'a, 'b> SmoltcpGossip<'a, 'b> {
  /// Build the view over the shared `sockets` for the gossip socket `udp`.
  pub(crate) fn new(sockets: &'a RefCell<&'a mut SocketSet<'b>>, udp: SocketHandle) -> Self {
    Self { sockets, udp }
  }
}

impl GossipIo for SmoltcpGossip<'_, '_> {
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    let mut set = self.sockets.borrow_mut();
    let sock = set.get_mut::<udp::Socket>(self.udp);
    // Pop the next deliverable datagram. `recv_slice` is called only while
    // `can_recv()` holds, so an empty rx ring is a clean `None` rather than an
    // `Exhausted` error to interpret.
    while sock.can_recv() {
      match sock.recv_slice(buf) {
        Ok((n, meta)) => return Some((from_endpoint(meta.endpoint), n)),
        // The datagram exceeded `buf` and was already POPPED by `recv_slice`
        // (smoltcp dequeues before the length check), so it is consumed and gone.
        // This is an over-budget peer datagram — larger than the configured gossip
        // MTU plus encryption overhead the buffer is sized for. Skip it and CONTINUE
        // draining the rest of the rx ring rather than returning, so one oversized
        // datagram cannot stall delivery of the in-budget datagrams queued behind it.
        Err(udp::RecvError::Truncated) => continue,
        // The ring is empty (`can_recv()` raced false): nothing more to deliver.
        Err(udp::RecvError::Exhausted) => return None,
      }
    }
    None
  }

  fn send(&mut self, bytes: &[u8], dest: SocketAddr) {
    let mut set = self.sockets.borrow_mut();
    let sock = set.get_mut::<udp::Socket>(self.udp);
    // Ignoring Err: gossip is best-effort — a full or errored UDP tx ring drops
    // this datagram and SWIM recovers on the next gossip round.
    let _ = sock.send_slice(bytes, to_endpoint(dest));
  }
}
