//! The per-slot mailbox bridging the synchronous engine
//! [`StreamIo`](memberlist_embedded::StreamIo) to an asynchronous embassy-net
//! `TcpSocket`.
//!
//! embassy-net's TCP API is async-only (`connect` / `accept` / `read` / `write`
//! all `.await`), but the engine drives its reliable plane through a synchronous
//! [`StreamIo`] that never awaits. The bridge is a fixed pool of slots: each slot
//! is a persistent async worker future owning one `TcpSocket`, paired with one
//! `RefCell<Mailbox>` that both the worker (async) and the engine (sync) touch.
//!
//! The mailbox is the only shared mutable state between the two. The engine's
//! [`pump`](memberlist_embedded::Engine::pump) is synchronous — every mailbox
//! borrow it takes completes before the pump returns and before any worker runs —
//! so the pump and the N workers are sibling futures in ONE task whose `RefCell`
//! borrows never overlap an `.await`.

use alloc::collections::VecDeque;
use core::net::SocketAddr;

/// A directive the engine (via [`EmbassyStream`](crate::stream_io::EmbassyStream))
/// posts to a slot's worker, consumed on the next worker wake.
///
/// Newtype/unit variants only: `Dial` carries the remote address; the rest are
/// state transitions with no payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Command {
  /// No pending directive. A free slot defaults to this; the worker re-listens
  /// (see [`crate::worker`]) so every idle slot is a candidate acceptor.
  Idle,
  /// Begin listening for an inbound connection on the given local port.
  Listen(u16),
  /// Begin dialing the given remote address.
  Dial(SocketAddr),
  /// Gracefully close (FIN, drain, then mark the slot closed).
  Close,
  /// Abort immediately (RST, discard buffered data).
  Abort,
}

/// The shared per-slot bridge state.
///
/// One `RefCell<Mailbox>` exists per pool slot. The engine reads/writes it
/// synchronously inside `pump`; the worker reads/writes it around its socket
/// awaits. The byte rings (`inbound` / `outbound`) are bounded — see
/// [`Mailbox::new`] — so a stalled peer cannot grow them without bound.
pub(crate) struct Mailbox {
  /// The directive the worker should act on next. The engine sets it; the worker
  /// consumes it (resetting to [`Command::Idle`] once acted on).
  pub command: Command,
  /// Whether the slot's connection has completed its handshake (worker set on a
  /// successful `connect` / `accept`). Mirrors `StreamIo::is_established`.
  pub established: bool,
  /// Whether the slot is still usable (`false` once the connection is
  /// closed/aborted/reset or a dial failed). The engine reaps a slot whose
  /// `open` is `false` via `StreamIo::is_open`.
  pub open: bool,
  /// The remote address of an accepted/connected peer, set by the worker ONLY
  /// once established with a known `remote_endpoint`. Mirrors smoltcp's accept
  /// gate (`StreamIo::accepted_peer`).
  pub accepted_peer: Option<SocketAddr>,
  /// Whether the worker observed the peer's FIN (a `read()` returning `Ok(0)` /
  /// the `Finished` receive state). Combined with a drained `inbound`, this is
  /// the one-shot EOF the engine reads via `StreamIo::recv_finished`.
  pub peer_fin: bool,
  /// The worker's mirror of `TcpSocket::send_queue()` — bytes written to the TX
  /// ring the peer has not yet ACKed. Feeds `StreamIo::send_queue` so the
  /// engine's drain-before-close gate is faithful.
  pub sock_send_queue: usize,
  /// Bytes the worker `read()` from the socket, awaiting `StreamIo::recv`.
  pub inbound: VecDeque<u8>,
  /// Bytes the engine `send()` enqueued, awaiting the worker's `write()`.
  pub outbound: VecDeque<u8>,
  /// Capacity bound for `inbound` (the configured TCP socket RX byte size).
  pub inbound_cap: usize,
  /// Capacity bound for `outbound` (the configured TCP socket TX byte size).
  pub outbound_cap: usize,
  /// Whether the slot's socket is in a clean, freshly-reset Closed state and so
  /// is safe to reuse for a new `Listen` / `Dial`.
  ///
  /// `true` for a fresh slot (a newly-created `TcpSocket` is Closed) and after the
  /// worker's `reset_socket` (`abort()` + flush, then [`Mailbox::reset`]); `false`
  /// from the instant the worker picks up a `Listen` / `Dial` (the socket is now in
  /// use) until that reset completes. The engine reads it via
  /// [`EmbassyStream::reuse_ready`](crate::stream_io::EmbassyStream) so it never
  /// re-`listen`s / re-`connect`s a slot whose teardown the worker has not yet run —
  /// which would clobber the pending `Abort`/`Close` and leak the prior connection's
  /// `open` / `accepted_peer` / buffers into the reused slot. This is the
  /// acknowledged reset→idle transition that makes the async teardown safe.
  pub reset_done: bool,
}

impl Mailbox {
  /// Build a fresh, idle mailbox with the given ring capacities.
  ///
  /// `inbound_cap` / `outbound_cap` come from the driver's
  /// [`Config`](crate::Config) (the TCP socket RX/TX byte sizes), so the bridge
  /// rings never hold more than a socket buffer's worth of un-handed-off bytes.
  pub(crate) fn new(inbound_cap: usize, outbound_cap: usize) -> Self {
    Self {
      command: Command::Idle,
      established: false,
      open: false,
      accepted_peer: None,
      peer_fin: false,
      sock_send_queue: 0,
      inbound: VecDeque::new(),
      outbound: VecDeque::new(),
      inbound_cap,
      outbound_cap,
      // A freshly-created `TcpSocket` is Closed, so the slot is immediately
      // reuse-ready for its first `Listen` / `Dial`.
      reset_done: true,
    }
  }

  /// Reset every per-connection field to the fresh/idle state, preserving the
  /// configured ring capacities. Called by the worker after a slot's connection
  /// breaks (close/abort/reset) so the slot is clean for its next reuse, and at
  /// the same point the engine returns the slot to its free-list.
  pub(crate) fn reset(&mut self) {
    self.command = Command::Idle;
    self.established = false;
    self.open = false;
    self.accepted_peer = None;
    self.peer_fin = false;
    self.sock_send_queue = 0;
    self.inbound.clear();
    self.outbound.clear();
    // The worker calls `reset()` only AFTER it has `abort()`ed + flushed the
    // socket back to a clean Closed state, so the slot is now reuse-ready.
    self.reset_done = true;
  }
}
