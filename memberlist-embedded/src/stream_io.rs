//! The reliable-plane pooled-stream I/O abstraction.

use core::net::SocketAddr;

/// A non-fatal stream-I/O fault surfaced by a [`StreamIo`] implementation.
#[derive(Debug)]
#[non_exhaustive]
pub enum StreamIoError {
  /// The address could not be used (e.g. an unspecified/zero endpoint).
  Unaddressable,
  /// The operation could not proceed now but may succeed on a later pump.
  Busy,
}

/// A generation tag identifying ONE occupancy of a pooled connection slot — the
/// span from the engine acquiring the slot (a [`listen`](StreamIo::listen) /
/// [`connect`](StreamIo::connect)) to the completion of that occupancy's teardown
/// ([`teardown_done`](StreamIo::teardown_done) returning `true`).
///
/// The engine assigns a fresh generation each time it takes a slot out of the
/// pool and NEVER advances a slot to its next generation before it has observed
/// [`teardown_done`](StreamIo::teardown_done) for the current one, so at most one
/// generation of a given slot is ever live. A driver keeps the tag only to answer
/// [`teardown_done`](StreamIo::teardown_done): it stamps the occupancy on the
/// gen-carrying calls and reports completion for the matching generation, so a
/// stale/mismatched query is inert.
///
/// The counter is a `u32` compared only for equality and wraps on overflow; an
/// ABA collision needs `2^32` completed-and-unobserved occupancies of one slot,
/// impossible under the engine's block-on-acknowledgement rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotGen(u32);

impl SlotGen {
  /// The first generation of any slot's first occupancy.
  pub const START: Self = Self(0);

  /// The generation following this one (wrapping).
  #[inline]
  pub const fn next(self) -> Self {
    Self(self.0.wrapping_add(1))
  }

  /// The raw counter value, for a driver's diagnostics.
  #[inline]
  pub const fn get(self) -> u32 {
    self.0
  }
}

impl Default for SlotGen {
  /// The default generation is [`START`](SlotGen::START).
  #[inline]
  fn default() -> Self {
    Self::START
  }
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

  /// Whether the teardown of occupancy `g` of slot `c` is complete — the socket
  /// is back in a state where a fresh [`listen`](StreamIo::listen) /
  /// [`connect`](StreamIo::connect) cannot clobber a pending teardown or suppress
  /// a pending RST/FIN — so the engine may free the slot and mint its next
  /// occupancy. This is the sole reuse gate; a slot is returned to the pool only
  /// once this reports `true`.
  ///
  /// It is a pure, non-blocking query. A mismatched or unknown generation MUST
  /// report `false`: the engine only ever queries the occupancy it is retiring,
  /// and a stale query (a slot already reused under a later generation) must
  /// never be mistaken for a completed teardown.
  ///
  /// A driver whose [`abort`](StreamIo::abort) / [`close`](StreamIo::close)
  /// completes on a later stack tick reports `false` until that teardown has
  /// actually finished — e.g. the smoltcp driver reports `false` for a socket
  /// aborted this tick whose RST has not yet egressed (`Closed` with its remote
  /// tuple still set), so the engine never reuses the handle before the reset
  /// packet reaches the wire; the embassy-net driver reports `false` until its
  /// per-slot worker has reset the socket and acknowledged the occupancy. A
  /// driver whose teardown is fully synchronous can report `true` as soon as the
  /// socket is `Closed`.
  fn teardown_done(&self, c: Self::Conn, g: SlotGen) -> bool;

  /// Begin listening for an inbound connection on `port` using slot `c` for
  /// occupancy `g`.
  fn listen(&mut self, c: Self::Conn, port: u16, g: SlotGen) -> Result<(), StreamIoError>;

  /// The remote address of a connection accepted on slot `c`, or `None` until a handshake completes.
  fn accepted_peer(&self, c: Self::Conn) -> Option<SocketAddr>;

  /// Begin dialing `remote` from `local_port` using slot `c` for occupancy `g`.
  fn connect(
    &mut self,
    c: Self::Conn,
    remote: SocketAddr,
    local_port: u16,
    g: SlotGen,
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

  /// Gracefully close occupancy `g` of slot `c` (send a FIN; let buffered data
  /// flush). The occupancy is not reusable until [`teardown_done`](StreamIo::teardown_done)
  /// reports `true` for `g`.
  fn close(&mut self, c: Self::Conn, g: SlotGen);

  /// Abort occupancy `g` of slot `c` immediately (reset the connection,
  /// discarding buffered data). The occupancy is not reusable until
  /// [`teardown_done`](StreamIo::teardown_done) reports `true` for `g`.
  fn abort(&mut self, c: Self::Conn, g: SlotGen);
}
