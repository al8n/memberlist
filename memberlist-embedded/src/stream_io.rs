//! The reliable-plane pooled-stream I/O abstraction.

use core::net::SocketAddr;

/// A non-fatal stream-I/O fault surfaced by a [`StreamIo`] implementation.
#[derive(Debug)]
pub enum StreamIoError {
  /// The address could not be used (e.g. an unspecified/zero endpoint).
  Unaddressable,
  /// The operation could not proceed now but may succeed on a later pump.
  Busy,
}

/// Non-blocking pooled reliable-stream I/O, keyed by an opaque per-socket handle.
///
/// A driver supplies this over a fixed pool of already-ticked reliable-stream
/// sockets; the engine owns the reliable-plane state machine and drives it
/// entirely through these methods, never touching a socket directly. Every
/// method is non-blocking and operates on a single connection identified by its
/// opaque [`Conn`](StreamIo::Conn) handle.
pub trait StreamIo {
  /// The opaque per-connection handle (e.g. a smoltcp `SocketHandle` or a pool slot id).
  type Conn: Copy + Eq;

  /// Take a free connection slot out of the pool, or `None` when the pool is exhausted.
  fn take_free(&mut self) -> Option<Self::Conn>;

  /// Return a connection slot to the free pool for reuse.
  fn give(&mut self, c: Self::Conn);

  /// The number of connection slots currently free in the pool.
  fn free_count(&self) -> usize;

  /// Whether slot `c`, currently in the free pool, is fully reset and safe to
  /// reuse for a fresh [`listen`](StreamIo::listen) / [`connect`](StreamIo::connect).
  ///
  /// A driver whose [`abort`](StreamIo::abort) / [`close`](StreamIo::close)
  /// completes synchronously (so the socket is back in a clean Closed state the
  /// moment the call returns — e.g. the smoltcp driver) always reports `true`,
  /// the default. A driver whose teardown is ASYNCHRONOUS — it posts the
  /// abort/close to a worker that resets the socket on a later wake (the
  /// embassy-net driver) — reports `false` until that worker has actually reset
  /// the socket and cleared its bridge state, so the engine never re-`listen`s /
  /// re-`connect`s a slot whose previous connection is still tearing down (which
  /// would clobber the pending abort and leak the prior connection's state into
  /// the reused slot). The engine consults this before reusing any pooled slot;
  /// a not-yet-ready slot is skipped this tick and retried once the worker has
  /// finished its reset.
  fn reuse_ready(&self, _c: Self::Conn) -> bool {
    true
  }

  /// Begin listening for an inbound connection on `port` using slot `c`.
  fn listen(&mut self, c: Self::Conn, port: u16) -> Result<(), StreamIoError>;

  /// The remote address of a connection accepted on slot `c`, or `None` until a handshake completes.
  fn accepted_peer(&self, c: Self::Conn) -> Option<SocketAddr>;

  /// Begin dialing `remote` from `local_port` using slot `c`.
  fn connect(
    &mut self,
    c: Self::Conn,
    remote: SocketAddr,
    local_port: u16,
  ) -> Result<(), StreamIoError>;

  /// Whether slot `c` is established and currently writable.
  fn may_send(&self, c: Self::Conn) -> bool;

  /// Whether slot `c` has buffered inbound bytes available to read.
  fn may_recv(&self, c: Self::Conn) -> bool;

  /// Whether slot `c` is not yet closed (used to detect a failed/torn-down connection).
  fn is_open(&self, c: Self::Conn) -> bool;

  /// Whether slot `c` has completed its handshake and reached the established state.
  fn is_established(&self, c: Self::Conn) -> bool;

  /// Read buffered inbound bytes from slot `c` into `buf`; `Some(len)` moved, or `None` if none ready.
  ///
  /// `None` is "no readable bytes this tick" and is NOT by itself an
  /// end-of-stream signal — an established connection with a momentarily empty
  /// receive ring also returns `None`. Distinguish the peer's graceful close
  /// with [`recv_finished`](StreamIo::recv_finished).
  fn recv(&mut self, c: Self::Conn, buf: &mut [u8]) -> Option<usize>;

  /// Whether the peer has gracefully closed its send half (a FIN was received)
  /// AND slot `c`'s receive buffer is fully drained — the end-of-stream signal.
  ///
  /// The reliable plane delivers exactly one EOF to the machine per connection,
  /// so this must report `true` only once every byte the peer sent before its
  /// FIN has already been handed back by [`recv`](StreamIo::recv). A driver over
  /// smoltcp returns the condition under which `tcp::Socket::recv_slice` yields
  /// `RecvError::Finished` (the receive half is closed with the buffer drained:
  /// any post-FIN state — `CloseWait` / `Closing` / `LastAck` / `TimeWait`). It
  /// must report `false` for a still-handshaking slot and for an established slot
  /// whose ring is merely empty, so no spurious EOF reaches the machine.
  ///
  /// A connection RESET (a received RST) is NOT a graceful end-of-stream and
  /// must report `false`: a reset is a transport FAILURE, surfaced via
  /// [`is_open`](StreamIo::is_open) going `false`, not a clean EOF. Reporting a
  /// reset as EOF would falsely complete an exchange the peer aborted — the
  /// machine maps a one-way `UserMessage` transport EOF to a *successful*
  /// completion. (Over smoltcp the RST-driven `Closed` state has no
  /// `rx_fin_received`, so it is excluded; over an async driver the worker sets
  /// `is_open == false` without the peer-FIN flag.)
  fn recv_finished(&self, c: Self::Conn) -> bool;

  /// Enqueue `bytes` for transmission on slot `c`; returns how many bytes were accepted.
  fn send(&mut self, c: Self::Conn, bytes: &[u8]) -> usize;

  /// Bytes written to slot `c`'s transmit ring that the peer has not yet
  /// acknowledged.
  ///
  /// The reliable plane's drain-before-close guarantee needs to know when every
  /// byte already handed to [`send`](StreamIo::send) has reached the peer, so a
  /// graceful FIN is never emitted ahead of an oversized push/pull reply still
  /// in flight. `0` means the transmit ring is fully acknowledged. A driver over
  /// smoltcp returns `tcp::Socket::send_queue`.
  fn send_queue(&self, c: Self::Conn) -> usize;

  /// Gracefully close slot `c` (send a FIN; let buffered data flush).
  fn close(&mut self, c: Self::Conn);

  /// Abort slot `c` immediately (reset the connection, discarding buffered data).
  fn abort(&mut self, c: Self::Conn);
}
