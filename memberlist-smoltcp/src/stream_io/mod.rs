//! The [`StreamIo`](memberlist_embedded::StreamIo) implementation over the pooled
//! smoltcp reliable-plane `tcp::Socket`s, keyed by [`SocketHandle`].
//!
//! A short-lived view, rebuilt each [`Memberlist::poll`](crate::Memberlist::poll)
//! over the already-ticked socket set and the [`Interface`] (the latter needed
//! only by `connect`, which threads the interface `context()` into smoltcp's
//! `tcp::Socket::connect`). The engine owns the reliable-plane state machine and
//! its connection pool ([`ReliablePlane`](memberlist_embedded::ReliablePlane)) and
//! drives the sockets entirely through this view; the stack tick (`iface.poll`)
//! the driver performs before handing the view to the engine.
//!
//! smoltcp keeps every socket in one [`SocketSet`], so this view shares mutable
//! access to it with the gossip view through a [`RefCell`] the driver holds for
//! one pump (see [`crate::gossip_io`]); each method takes a brief `borrow`/`borrow_mut`,
//! never holding a socket borrow across a call into the other view.
//!
//! # Pool ownership
//!
//! The [`StreamIo`] trait exposes `take_free` / `give` / `free_count` for a driver
//! that backs its connection slots from its OWN free-list. This driver does not:
//! the engine's [`ReliablePlane`](memberlist_embedded::ReliablePlane) owns the
//! `SocketHandle` free-list (seeded at construction via `plane_mut().pool.push`)
//! and the engine reaches it directly, never through these three trait methods, so
//! for this view they are inert (see their impls). Every socket-touching operation
//! the engine DOES route through the view — `listen` / `connect` / `recv` / `send`
//! / `close` / `abort` and the lifecycle predicates — is implemented faithfully
//! below.

use core::{cell::RefCell, net::SocketAddr};

use memberlist_embedded::{StreamIo, StreamIoError};
use smoltcp::{
  iface::{Interface, SocketHandle, SocketSet},
  socket::tcp,
};

use crate::addr::{from_endpoint, to_endpoint};

/// A [`StreamIo`] view over the pooled reliable-plane `tcp::Socket`s in a shared
/// [`SocketSet`], plus the [`Interface`] needed to dial.
///
/// Resolves each socket by its [`SocketHandle`], taking a brief `borrow`/`borrow_mut`
/// of the shared set; `connect` additionally borrows the interface for its context.
pub(crate) struct SmoltcpStream<'a, 'b> {
  iface: &'a mut Interface,
  sockets: &'a RefCell<&'a mut SocketSet<'b>>,
}

impl<'a, 'b> SmoltcpStream<'a, 'b> {
  /// Build the view over `iface` and the shared `sockets`.
  pub(crate) fn new(iface: &'a mut Interface, sockets: &'a RefCell<&'a mut SocketSet<'b>>) -> Self {
    Self { iface, sockets }
  }
}

impl StreamIo for SmoltcpStream<'_, '_> {
  type Conn = SocketHandle;

  // The engine owns the `SocketHandle` free-list inside its `ReliablePlane` and
  // reaches it directly, so it never calls these three on the view. They are
  // implemented as no-ops that report an empty pool: there is no driver-side
  // free-list for this view to draw from, and reporting one would be a lie that a
  // hypothetical future caller could misread. The single authority is the engine's
  // `ReliablePlane::pool`.

  fn take_free(&mut self) -> Option<Self::Conn> {
    None
  }

  fn give(&mut self, _c: Self::Conn) {}

  fn free_count(&self) -> usize {
    0
  }

  fn listen(&mut self, c: Self::Conn, port: u16) -> Result<(), StreamIoError> {
    // `listen()` fails on port 0 (`Unaddressable`) or an already-open socket
    // (`InvalidState`). Map both to a non-fatal error rather than panicking; the
    // engine's listener paths only ever pass a non-zero port and a freshly-reset
    // (Closed) socket, so neither fires in practice.
    self
      .sockets
      .borrow_mut()
      .get_mut::<tcp::Socket>(c)
      .listen(port)
      .map_err(|_| StreamIoError::Unaddressable)
  }

  fn accepted_peer(&self, c: Self::Conn) -> Option<SocketAddr> {
    let set = self.sockets.borrow();
    let sock = set.get::<tcp::Socket>(c);
    // The accept gate is `may_send()`, true only in Established and CloseWait. Both
    // mean the handshake settled and our send half is open (CloseWait additionally
    // covers a peer that already half-closed after sending its push/pull half —
    // still a completed, accept-worthy connection).
    //
    // `is_active()` MUST NOT be used here: it is also true in SynReceived (the
    // half-open state after we send the SYN-ACK but before the remote's final ACK).
    // Accepting in SynReceived would move a not-yet-established socket out of the
    // listener slot while it still carries the listen endpoint; a retransmit/RST
    // during the unfinished handshake then flips that socket back to Listen (smoltcp
    // `tcp.rs`: an RST in SynReceived with a non-zero listen_endpoint reverts to
    // Listen), silently turning the exchange's socket into a second listener and
    // wedging the join. Gating on `may_send()` accepts strictly at/after Established,
    // where an RST closes the socket cleanly instead of reverting it.
    if !sock.may_send() {
      return None;
    }
    sock.remote_endpoint().map(from_endpoint)
  }

  fn connect(
    &mut self,
    c: Self::Conn,
    remote: SocketAddr,
    local_port: u16,
  ) -> Result<(), StreamIoError> {
    // smoltcp's `connect` needs the interface context to resolve the local
    // endpoint. The interface is a separate field from the socket set, so the
    // `&mut self.iface` context borrow and the socket set's `borrow_mut` do not
    // conflict.
    let remote_ep = to_endpoint(remote);
    let cx = self.iface.context();
    self
      .sockets
      .borrow_mut()
      .get_mut::<tcp::Socket>(c)
      .connect(cx, remote_ep, local_port)
      .map_err(|_| StreamIoError::Unaddressable)
  }

  fn may_send(&self, c: Self::Conn) -> bool {
    self.sockets.borrow().get::<tcp::Socket>(c).may_send()
  }

  fn may_recv(&self, c: Self::Conn) -> bool {
    self.sockets.borrow().get::<tcp::Socket>(c).may_recv()
  }

  fn is_open(&self, c: Self::Conn) -> bool {
    self.sockets.borrow().get::<tcp::Socket>(c).is_open()
  }

  fn is_established(&self, c: Self::Conn) -> bool {
    // The engine's `promote_established` check is "send-capable" — Established (and
    // also CloseWait if the peer FIN'd before we did). That is exactly smoltcp's
    // `may_send()`, so mirror it rather than re-deriving from `state()`.
    self.sockets.borrow().get::<tcp::Socket>(c).may_send()
  }

  fn recv_finished(&self, c: Self::Conn) -> bool {
    // True iff `tcp::Socket::recv_slice` would return `RecvError::Finished`, which
    // smoltcp's `recv_error_check` defines as `!may_recv() && rx_fin_received`. A
    // graceful peer FIN (a clean EOF) must report `true`; a connection RESET (RST)
    // must report `false` — a reset is a failure, not an orderly end-of-stream.
    //
    // `rx_fin_received` is private, so derive it from the public `state()`. A
    // received FIN sets the flag and moves the socket into exactly one of CloseWait
    // (from Established/SynReceived), Closing (from FinWait1), LastAck (CloseWait
    // after our own `close()`), or TimeWait (from FinWait1/FinWait2); those post-FIN
    // states persist until our side FINs and the peer ACKs (or the 2MSL TimeWait
    // elapses), so the engine's per-pump inbound drain always observes the EOF in one
    // of them BEFORE the socket can reach `Closed`. `Closed` is therefore EXCLUDED:
    // smoltcp enters `Closed` on a received RST WITHOUT setting `rx_fin_received`
    // (`socket/tcp.rs`: the RST arm and `reset()` both clear it / never set it), so a
    // RST-driven `Closed` is `recv_error_check == InvalidState`, NOT `Finished`.
    // Mapping `Closed` to `true` would turn a reset into a clean EOF, silently
    // completing an exchange the peer actually aborted; excluding it loses no real
    // FIN because the graceful EOF was already delivered from a post-FIN state above.
    //
    // `!may_recv()` is the second term: it is false while the rx ring still holds
    // bytes (`may_recv()` is true via `can_recv()` there), so this reports `false`
    // until every byte the peer sent before its FIN has been drained — delivering
    // the EOF exactly once, after the data, never before. It is also false while
    // handshaking and for an open-but-empty Established ring, so no spurious EOF
    // reaches the machine.
    let set = self.sockets.borrow();
    let sock = set.get::<tcp::Socket>(c);
    matches!(
      sock.state(),
      tcp::State::CloseWait | tcp::State::Closing | tcp::State::LastAck | tcp::State::TimeWait
    ) && !sock.may_recv()
  }

  fn recv(&mut self, c: Self::Conn, buf: &mut [u8]) -> Option<usize> {
    match self
      .sockets
      .borrow_mut()
      .get_mut::<tcp::Socket>(c)
      .recv_slice(buf)
    {
      // `Ok(0)` is a momentarily empty Established ring (no data this tick); the
      // engine treats `None` and `Some(0)` identically, but reporting `None` keeps
      // the contract crisp ("no readable bytes this tick").
      Ok(0) => None,
      Ok(n) => Some(n),
      // EOF is delivered via `recv_finished`, never via `recv`: map both the drained
      // peer-FIN (`Finished`) and the not-yet-receivable handshaking socket
      // (`InvalidState`) to `None`. The engine consults `recv_finished` on a `None`
      // read to deliver the one-shot EOF.
      Err(tcp::RecvError::Finished) | Err(tcp::RecvError::InvalidState) => None,
    }
  }

  fn send(&mut self, c: Self::Conn, bytes: &[u8]) -> usize {
    // `send_slice` accepts as many bytes as the tx ring has room for (a partial
    // write on a full ring) and errors only with `InvalidState` when the tx half is
    // not open. The engine gates this call on `may_send()` (it skips a
    // not-send-capable socket in `pump_outbound_reliable`), so the error path is
    // unreachable here; treat any error as "0 bytes accepted" so a torn-down tx half
    // simply makes no progress rather than panicking.
    // Ignoring Err: the engine only sends on a send-capable socket; 0 = no progress.
    self
      .sockets
      .borrow_mut()
      .get_mut::<tcp::Socket>(c)
      .send_slice(bytes)
      .unwrap_or(0)
  }

  fn send_queue(&self, c: Self::Conn) -> usize {
    self.sockets.borrow().get::<tcp::Socket>(c).send_queue()
  }

  fn close(&mut self, c: Self::Conn) {
    self.sockets.borrow_mut().get_mut::<tcp::Socket>(c).close();
  }

  fn abort(&mut self, c: Self::Conn) {
    self.sockets.borrow_mut().get_mut::<tcp::Socket>(c).abort();
  }
}

#[cfg(test)]
mod tests;
