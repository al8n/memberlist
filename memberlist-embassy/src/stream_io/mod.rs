//! The [`StreamIo`](memberlist_embedded::StreamIo) implementation over the
//! per-slot mailboxes, keyed by [`SlotId`].
//!
//! A short-lived view the [`Runner`](crate::Runner) rebuilds each engine pump
//! over the slot mailboxes, the free-list, and the per-slot worker wake signals.
//! The engine drives the reliable plane entirely through this view; it NEVER
//! touches a `TcpSocket` — that is the worker's job (see [`crate::worker`]). Each
//! method takes a brief `borrow`/`borrow_mut` of one mailbox and, for the
//! command-posting methods, fires that slot's wake so its worker promptly acts on
//! the new directive.
//!
//! Because the engine's [`pump`](memberlist_embedded::Engine::pump) is
//! synchronous, every borrow these methods take completes before `pump` returns —
//! and before any worker future runs — so the view's borrows never overlap a
//! worker's.

use core::{cell::RefCell, net::SocketAddr};

use embassy_sync::{blocking_mutex::raw::NoopRawMutex, signal::Signal};
use memberlist_embedded::{StreamIo, StreamIoError};

use crate::mailbox::{Command, Mailbox};

/// An opaque pool-slot identifier — the engine's reliable-plane connection
/// handle (`StreamIo::Conn`) for this driver. A plain index into the slot arrays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(pub(crate) usize);

impl SlotId {
  /// The raw slot index.
  #[inline]
  pub const fn index(self) -> usize {
    self.0
  }
}

/// The per-slot wake signal type: a one-shot `()` signal on the single-executor
/// (`!Send`) [`NoopRawMutex`], fired by the engine view to nudge a slot's worker.
pub(crate) type SlotWake = Signal<NoopRawMutex, ()>;

/// A [`StreamIo`] view over the per-slot mailboxes plus the pool free-list and
/// the worker wake signals.
///
/// Rebuilt by the [`Runner`](crate::Runner) each pump. Holds shared references to
/// the per-slot `RefCell<Mailbox>`es and wake [`Signal`]s, and a mutable
/// reference to the driver-owned free-list of [`SlotId`]s.
pub struct EmbassyStream<'a> {
  /// One mailbox per pool slot, indexed by [`SlotId`].
  pub(crate) slots: &'a [RefCell<Mailbox>],
  /// One command wake per pool slot, indexed by [`SlotId`]. Firing slot `c`'s
  /// wake nudges exactly that slot's worker to act on the freshly-posted command.
  pub(crate) cmd_wakes: &'a [SlotWake],
  /// The driver-owned free-list of currently-unassigned slots.
  pub(crate) free: &'a mut alloc::vec::Vec<SlotId>,
}

impl<'a> EmbassyStream<'a> {
  /// Build the view over the slot mailboxes, command wakes, and free-list.
  #[inline]
  pub(crate) fn new(
    slots: &'a [RefCell<Mailbox>],
    cmd_wakes: &'a [SlotWake],
    free: &'a mut alloc::vec::Vec<SlotId>,
  ) -> Self {
    Self {
      slots,
      cmd_wakes,
      free,
    }
  }

  /// Post a command to slot `c`'s mailbox and wake its worker.
  #[inline]
  fn post(&self, c: SlotId, cmd: Command) {
    self.slots[c.0].borrow_mut().command = cmd;
    self.cmd_wakes[c.0].signal(());
  }
}

impl StreamIo for EmbassyStream<'_> {
  type Conn = SlotId;

  // ── Pool (driver-owned free-list) ───────────────────────────────────────────
  //
  // Unlike the smoltcp view (whose engine owns the `SocketHandle` free-list and
  // never calls these), this driver backs its slots from its OWN `free` Vec, so
  // these three are implemented faithfully. The engine's `ReliablePlane::pool`
  // is still seeded with every `SlotId` at construction; `take_free` / `give`
  // here are the driver-side mirror the engine uses through the view when it
  // routes pool churn through `StreamIo` (the embedded engine seeds and reaches
  // its own pool, so in practice these are exercised by the engine's pool calls
  // that go through the trait — kept correct so a slot is never double-issued).

  fn take_free(&mut self) -> Option<Self::Conn> {
    self.free.pop()
  }

  fn give(&mut self, c: Self::Conn) {
    self.free.push(c);
  }

  fn free_count(&self) -> usize {
    self.free.len()
  }

  fn reuse_ready(&self, c: Self::Conn) -> bool {
    // embassy-net teardown is ASYNCHRONOUS: `abort`/`close` post a command to the
    // slot's worker, which resets the `TcpSocket` on a later wake — so a slot the
    // engine just `give`s back to the pool is NOT yet clean. The worker clears
    // `reset_done` the instant it begins a `Listen`/`Dial` and re-sets it only after
    // `reset_socket` (`abort()` + flush + mailbox clear), so this gate is `true`
    // exactly when the slot's socket is back in a clean Closed state. The engine
    // consults it before reusing any pooled slot, so a freed-but-still-tearing-down
    // slot is skipped this tick and retried once its worker has finished the reset —
    // never re-`listen`ed/`connect`ed over a pending teardown (which would leak the
    // prior connection's `open`/`accepted_peer`/buffers into the reused slot).
    self.slots[c.0].borrow().reset_done
  }

  // ── Listener / accept ───────────────────────────────────────────────────────

  fn listen(&mut self, c: Self::Conn, port: u16) -> Result<(), StreamIoError> {
    // embassy-net binds its listen endpoint to the port; port 0 is rejected by
    // `accept` (`InvalidPort`). The engine only ever passes its non-zero bound
    // port, so this never fires in practice; surface a non-fatal error rather
    // than asserting.
    if port == 0 {
      return Err(StreamIoError::Unaddressable);
    }
    self.post(c, Command::Listen(port));
    Ok(())
  }

  fn accepted_peer(&self, c: Self::Conn) -> Option<SocketAddr> {
    // The worker sets `accepted_peer` to `Some` ONLY once the slot is established
    // with a known remote endpoint (the embassy-net accept gate), matching
    // smoltcp's `may_send()`-gated `remote_endpoint()`.
    self.slots[c.0].borrow().accepted_peer
  }

  // ── Dial ────────────────────────────────────────────────────────────────────

  fn connect(
    &mut self,
    c: Self::Conn,
    remote: SocketAddr,
    _local_port: u16,
  ) -> Result<(), StreamIoError> {
    // `_local_port` is unused: embassy-net's `TcpSocket::connect` binds its own
    // ephemeral local port from the stack (`get_local_port`), so the engine's
    // requested local port has no effect here. (smoltcp threaded it through; the
    // embassy-net stack owns ephemeral-port selection.)
    self.post(c, Command::Dial(remote));
    Ok(())
  }

  // ── Lifecycle predicates ──────────────────────────────────────────────────────

  fn may_send(&self, c: Self::Conn) -> bool {
    // Established AND the outbound ring has room for at least one more byte. The
    // engine gates `send` on this; reporting capacity here keeps it from
    // attempting a push that the bounded ring would partially reject.
    let mb = self.slots[c.0].borrow();
    mb.established && mb.outbound.len() < mb.outbound_cap
  }

  fn may_recv(&self, c: Self::Conn) -> bool {
    // Mirrors the smoltcp view's `may_recv()`: readable while established and not
    // (peer-FIN'd with the inbound ring drained), OR whenever buffered inbound
    // bytes remain (so a half-closed peer's already-delivered bytes still read).
    let mb = self.slots[c.0].borrow();
    (mb.established && !(mb.peer_fin && mb.inbound.is_empty())) || !mb.inbound.is_empty()
  }

  fn is_open(&self, c: Self::Conn) -> bool {
    self.slots[c.0].borrow().open
  }

  fn is_established(&self, c: Self::Conn) -> bool {
    self.slots[c.0].borrow().established
  }

  fn recv_finished(&self, c: Self::Conn) -> bool {
    // The one-shot EOF: the worker saw the peer's FIN AND the engine has drained
    // every byte the peer sent before it (`inbound` empty). False while the ring
    // still holds bytes, while handshaking (`peer_fin` unset), and for an
    // established-but-momentarily-empty ring — so no spurious EOF reaches the
    // machine and the FIN is delivered exactly once, after the data.
    let mb = self.slots[c.0].borrow();
    mb.peer_fin && mb.inbound.is_empty()
  }

  // ── Byte I/O ──────────────────────────────────────────────────────────────────

  fn recv(&mut self, c: Self::Conn, buf: &mut [u8]) -> Option<usize> {
    // Drain up to `buf.len()` bytes from the inbound ring. `None` when empty —
    // "no readable bytes this tick", NOT end-of-stream (that is `recv_finished`).
    let (drained, was_full) = {
      let mut mb = self.slots[c.0].borrow_mut();
      if mb.inbound.is_empty() {
        return None;
      }
      let was_full = mb.inbound.len() >= mb.inbound_cap;
      let mut n = 0;
      while n < buf.len() {
        match mb.inbound.pop_front() {
          Some(b) => {
            buf[n] = b;
            n += 1;
          }
          None => break,
        }
      }
      (n, was_full)
    };
    // Wake the worker if this drain relieved a full inbound ring, so a worker
    // parked on backpressure resumes reading from the socket. No-op in the common
    // case where the ring had room.
    if was_full {
      self.cmd_wakes[c.0].signal(());
    }
    Some(drained)
  }

  fn send(&mut self, c: Self::Conn, bytes: &[u8]) -> usize {
    // Push into the outbound ring up to its capacity; return how many bytes were
    // accepted (a partial accept on a near-full ring, like a TCP tx ring). Wake
    // the worker so it writes the freshly-queued bytes without waiting for an
    // unrelated event.
    let mut mb = self.slots[c.0].borrow_mut();
    let room = mb.outbound_cap.saturating_sub(mb.outbound.len());
    let take = room.min(bytes.len());
    mb.outbound.extend(bytes[..take].iter().copied());
    drop(mb);
    if take > 0 {
      self.cmd_wakes[c.0].signal(());
    }
    take
  }

  fn send_queue(&self, c: Self::Conn) -> usize {
    // Un-written bytes (still in the outbound ring) PLUS written-but-unACKed
    // bytes (the worker's mirror of `TcpSocket::send_queue()`). Reaches 0 only
    // when both rings have fully drained to the peer, so the engine's
    // drain-before-close gate never FINs ahead of an in-flight reply.
    let mb = self.slots[c.0].borrow();
    mb.outbound.len() + mb.sock_send_queue
  }

  // ── Close ─────────────────────────────────────────────────────────────────────

  fn close(&mut self, c: Self::Conn) {
    self.post(c, Command::Close);
  }

  fn abort(&mut self, c: Self::Conn) {
    self.post(c, Command::Abort);
  }
}

#[cfg(test)]
mod tests;
