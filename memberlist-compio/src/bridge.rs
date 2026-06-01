//! Per-bridge byte-mover task — owns the negotiated stream after handshake
//! completion and drives it until the driver closes the bridge.
//!
//! Uses [`compio::io::Splittable`] so the recv and send halves are
//! independent: for [`compio::net::TcpStream`] the split is native and
//! lock-free (both halves are `TcpStream` clones); for a TLS stream wrapped
//! via [`compio::io::split`] the halves share an intrinsic `BiLock` because
//! the TLS record layer cannot be touched from two tasks concurrently.
//!
//! ## Push/pull half-close lifecycle
//!
//! The reliable exchange is a one-shot request-response. Each side:
//!
//! 1. Writes its half of the exchange (push request OR pull response).
//! 2. Half-closes its write side (driven by [`BridgeOut::ShutdownWrite`])
//!    so the peer's read side sees FIN — the peer learns "I am done
//!    sending on this exchange".
//! 3. Continues reading the peer's bytes on its still-open read half
//!    until the peer reciprocates with its own FIN (`read == 0`).
//! 4. Signals EOF to the driver so the coordinator can advance the
//!    membership FSM through the peer's reply.
//! 5. Stays alive — accepting any late `Bytes` the coordinator surfaces
//!    after observing the peer's EOF (the inbound-server side writes
//!    its response AFTER the request EOF, not before) — until the
//!    driver sends [`BridgeOut::Close`].
//!
//! A bridge that exited on peer-EOF would drop the inbound-server's
//! response on the floor: the driver pushes the response into `out_tx`
//! AFTER the coordinator processes the request, but the bridge would
//! already be gone, the out-channel would be disconnected, and
//! `drain_transport_transmits` would silently drop the bytes.
//!
//! ## Flush-then-close ordering
//!
//! `out_rx` is a single channel carrying every [`BridgeOut`] variant in
//! FIFO order. The bridge processes one item per iteration, so every
//! byte the driver queued before a `ShutdownWrite` or `Close` is written
//! to the peer before the close signal fires. Folding a hard abort into
//! this same FIFO would defeat the discard requirement — the abort would
//! queue BEHIND the stale bytes and the bridge would flush them first —
//! so the hard abort rides a SEPARATE out-of-band `cancel_rx` channel
//! that the loop selects on with priority (see below).
//!
//! ## Hard abort (`cancel_rx`)
//!
//! Only a FAILED exchange (`StreamAction::Abort`) aborts: the driver
//! sends an explicit `cancel_tx.send(())`, which resolves `cancel_rx`
//! with `Ok(())`. A single cancel future, hoisted above the loop, resolves
//! ONLY on that explicit `Ok(())`; it is wired into the loop two ways:
//!
//! 1. The `select_biased!` boundary lists the cancel arm FIRST, so an
//!    abort resolving while the bridge is between writes wins ahead of
//!    both `out_rx` and the read half.
//! 2. Each partial write is RACED against the same cancel future (see
//!    [`write_cancellable`]), so an abort that arrives while a write is
//!    already in flight drops the write future at its next await point.
//!
//! Either way the bridge breaks immediately and drops its write half
//! WITHOUT draining the `Bytes` still queued in `out_rx` and WITHOUT
//! sending an EOF marker. Those queued bytes are stale (belonging to an
//! exchange the coordinator gave up on) and are discarded with the dropped
//! `out_rx`. The mid-write race is what makes the abort PROMPT even when
//! the peer stopped reading and a write would otherwise block
//! forever — leaking the bridge task and its socket.
//!
//! A graceful `StreamAction::Close` is NOT an abort. It queues
//! `BridgeOut::Close` behind the response `Bytes` and then drops the
//! `BridgeHandle` (and `cancel_tx`), so `cancel_rx` resolves with
//! `Err(Disconnected)`. That disconnect must not preempt the FIFO — a
//! disconnect winning the bias over already-queued `Bytes`/`Close`, or
//! dropping a write mid-flight, would truncate a clean exchange's final
//! response. So the cancel future maps the disconnect to a future that
//! NEVER resolves: the cancel arm can no longer fire on a graceful Close
//! and no write is ever dropped mid-flight (no partial-write / duplication
//! hazard). The graceful drain+teardown is driven SOLELY by `out_rx`'s
//! FIFO (`Bytes` then `Close`/`Err`).
//!
//! ## Graceful-drain backstop (`close_timeout`)
//!
//! A graceful `StreamAction::Close` has NO remaining cancel path: the driver
//! removed the bridge handle, so it can never send an Abort for this exchange.
//! If the peer sent a valid request+FIN and then STOPPED reading, its receive
//! window collapses to zero and the post-Close drain blocks FOREVER — the
//! machine already emitted Close and forgot the exchange, and the bridge task
//! is detached, so sustained churn accumulates wedged streams and tasks (a leak
//! / DoS vector). To bound it, the drain is a chunked write loop and EACH
//! partial write is raced against a `compio::time::sleep(close_timeout)` that is
//! re-armed FRESH on every chunk. Because progress resets the deadline, this is
//! a NO-PROGRESS (idle) timeout, not a cap on total drain duration: a peer that
//! keeps reading — even slowly, so a large frame takes longer than
//! `close_timeout` overall — advances on every chunk and never trips it. It
//! fires only when a single partial write makes NO progress for the full
//! `close_timeout` (the peer is not reading at all); the bridge then tears down,
//! dropping its write half so the OS RSTs the stuck stream. It never preempts a
//! draining write that is still making progress, or a clean exchange.
//!
//! ## Cancellation model
//!
//! The recv future owns the read buffer by value while it is in flight;
//! if the out arm fires before recv completes, the recv future is dropped
//! (compio cancels the in-flight syscall via `Submit::drop`) and a fresh
//! buffer is allocated on the next loop iteration. The buffer allocation
//! on the cancellation path is deliberate: keeping the recv future pinned
//! across iterations with a stashed buffer requires an unnameable-future
//! slot that adds complexity without affecting correctness.

use std::{future::Future, io, time::Duration};

use memberlist_proto::Instant;

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  io::{AsyncRead, AsyncWrite, util::Splittable},
};
use flume::{Receiver, Sender};
use futures_util::{FutureExt, future::FusedFuture, pin_mut, select_biased};

use crate::driver::{BridgeBytes, BridgeEof, BridgeError, BridgeInbound, BridgeOut, ExchangeId};

// Per-bridge recv buffer size is now configured by the driver via
// [`crate::DriverOptions::with_bridge_recv_buf_len`]; the historical
// default value lives at
// [`crate::DEFAULT_BRIDGE_RECV_BUF_LEN`].

/// Run the per-bridge byte-mover loop until a [`BridgeOut::Close`] arrives,
/// the driver signals `cancel_rx` (hard abort), the driver drops the
/// out-channel sender, or the stream returns an I/O error.
///
/// Ownership transfer:
/// - `stream`: the post-handshake stream value (TcpStream or
///   `Split<TlsStream>`).
/// - `eid`: the coordinator-allocated handle for tagging inbound events.
/// - `out_rx`: the driver's bytes-or-control FIFO into this bridge.
/// - `cancel_rx`: the driver's out-of-band hard-abort signal. An explicit
///   `cancel_tx.send(())` (the `StreamAction::Abort` arm for a FAILED
///   exchange) resolves the hoisted cancel future with PRIORITY over both
///   `out_rx` and the read half, AND preempts a write already in
///   flight, so the bridge breaks immediately and drops its write half
///   WITHOUT draining any `Bytes` still queued in `out_rx` — those stale
///   bytes are discarded, not written. A graceful-Close handle-drop
///   resolves `recv_async()` with `Err(Disconnected)` instead; that is NOT
///   an abort and is mapped to a never-resolving future, so it neither
///   fires the cancel arm nor drops a write mid-flight — the queued `Bytes`
///   then `Close` in `out_rx` flush first.
/// - `inbound_tx`: bytes / eof / error events the driver consumes.
/// - `close_timeout`: no-progress (idle) bound on a drain. A graceful Close has
///   no remaining cancel path, so a peer that stopped reading would wedge the
///   drain forever; if a single partial write makes NO progress for this long
///   the drain is abandoned and the bridge tears down (RST). A peer that keeps
///   reading — even slowly enough that the whole frame outlasts `close_timeout`
///   — resets the deadline on every chunk and never trips it.
///
/// All `Sender::send_async` failures are ignored with a justification: if
/// the driver dropped the inbound receiver we are already shutting down
/// and the event can be discarded.
pub(crate) async fn bridge_task<S>(
  stream: S,
  eid: ExchangeId,
  out_rx: Receiver<BridgeOut>,
  cancel_rx: Receiver<()>,
  inbound_tx: Sender<BridgeInbound>,
  recv_buf_len: usize,
  close_timeout: Duration,
) where
  S: Splittable,
  S::ReadHalf: AsyncRead,
  S::WriteHalf: AsyncWrite,
{
  let (mut read_half, mut write_half) = stream.split();
  let mut write_closed = false;
  let mut read_closed = false;

  // Hoist a single cancel future ABOVE the loop that resolves ONLY on an
  // explicit abort. An explicit `cancel_tx.send(())` — emitted by
  // `StreamAction::Abort` for a FAILED exchange — resolves `recv_async()`
  // with `Ok(())`, which both wins the select bias at the boundary AND
  // preempts a write already in flight (the write arms race against this
  // same future). A graceful `StreamAction::Close` instead drops the
  // `BridgeHandle` (and thus `cancel_tx`) AFTER queuing `BridgeOut::Close`
  // behind the response `Bytes`, which resolves `recv_async()` with
  // `Err(Disconnected)`; that disconnect is NOT an abort, so it is mapped
  // to a future that NEVER resolves. Mapping the disconnect to `pending()`
  // means the cancel future can never win a write race on a graceful close
  // (no write is dropped mid-flight — no partial-write / duplication
  // hazard); the graceful drain+teardown is driven SOLELY by `out_rx`'s
  // FIFO (`Bytes` then `Close`/`Err`). Fusing keeps the future
  // `is_terminated()`-safe across the many `select_biased!` re-polls.
  let cancel_fut = async {
    match cancel_rx.recv_async().await {
      Ok(()) => (),
      Err(_) => futures_util::future::pending::<()>().await,
    }
  }
  .fuse();
  pin_mut!(cancel_fut);

  loop {
    if read_closed {
      // Read-closed mode: peer FIN already observed and the matching
      // EOF marker already sent to the driver. The bridge stays alive
      // to accept any late `Bytes` the coordinator surfaces (the
      // server side writes its response AFTER the request EOF).
      // Exits on a hard abort (`cancel_rx`), `Close`, sender drop, or
      // the next write error.
      let out_fut = out_rx.recv_async();
      pin_mut!(out_fut);
      select_biased! {
        // Cancel first: ONLY an explicit abort can resolve this future now
        // (a graceful-Close disconnect is mapped to `pending()`), so it
        // breaks immediately, dropping the write half WITHOUT writing any
        // queued response `Bytes`. The graceful drain+teardown is driven
        // SOLELY by the `out_rx` arm's `Close`/`Err` path.
        () = &mut cancel_fut => break,
        out_msg = out_fut => match out_msg {
          Ok(BridgeOut::Bytes(bytes)) => {
            if write_closed {
              // Discard bytes after a half-close — the machine guarantees
              // it stops queueing for this exchange after `ShutdownWrite`,
              // but the gate is defensive against any in-flight chunk
              // that raced past the channel boundary.
              continue;
            }
            // Race each partial write against the cancel future AND a fresh
            // `close_timeout`. An explicit abort preempts a write already in
            // flight; the idle timeout bounds a post-Close drain whose peer
            // stopped reading (it has no remaining cancel path and would
            // otherwise block forever, leaking the bridge and socket).
            // `cancel_fut` resolves only on a real abort, so a graceful Close
            // never drops a progressing write mid-flight — and because progress
            // resets the deadline, only a peer making NO progress for the full
            // `close_timeout` times out.
            match write_cancellable(&mut write_half, bytes, &mut cancel_fut, close_timeout).await {
              // Aborted mid-write, or the drain made no progress for
              // `close_timeout`: discard and tear down (drop the write half →
              // RST), no EOF.
              WriteOutcome::Aborted | WriteOutcome::TimedOut => break,
              WriteOutcome::Wrote(res) => {
                if let Err(err) = res {
                  // Ignoring Err: shutdown race — driver-side receiver may
                  // be gone if the driver is tearing down.
                  let _ = inbound_tx
                    .send_async(BridgeInbound::Error(BridgeError {
                      eid,
                      err,
                      received_at: Instant::now(),
                    }))
                    .await;
                  break;
                }
              }
            }
          }
          Ok(BridgeOut::ShutdownWrite) => {
            if !write_closed {
              // Ignoring Err: a shutdown on a socket the peer FIN'd is
              // non-actionable.
              let _ = write_half.shutdown().await;
              write_closed = true;
            }
          }
          Ok(BridgeOut::Close) | Err(_) => {
            // Graceful teardown (explicit `Close` or the handle-drop
            // disconnect). No EOF send here — the read-EOF transition in
            // the both-halves-live arm already sent one. Just exit.
            break;
          }
        },
      }
      continue;
    }

    // Both-halves-live mode: select between a hard abort, out_rx, and read.
    let buf = vec![0u8; recv_buf_len];
    let recv_fut = read_half.read(buf).fuse();
    let out_fut = out_rx.recv_async();
    pin_mut!(recv_fut, out_fut);

    select_biased! {
      // Cancel first: ONLY an explicit abort (`StreamAction::Abort` for a
      // FAILED exchange) can resolve this future now — a graceful-Close
      // disconnect is mapped to `pending()`. It breaks immediately, dropping
      // both halves WITHOUT draining any `Bytes` still queued in `out_rx`
      // and WITHOUT sending an EOF marker — the driver already removed the
      // bridge handle and treats the exchange as failed. The graceful
      // drain+teardown is driven SOLELY by the `out_rx` arm's `Close`/`Err`
      // path.
      () = &mut cancel_fut => break,
      out_msg = out_fut => match out_msg {
        Ok(BridgeOut::Bytes(bytes)) => {
          if write_closed {
            continue;
          }
          // Race each partial write against the cancel future AND a fresh
          // `close_timeout`. An explicit abort preempts a write already in
          // flight; the idle timeout bounds a post-Close drain whose peer
          // stopped reading (no remaining cancel path, else it blocks forever,
          // leaking the bridge and socket). `cancel_fut` resolves only on a
          // real abort, so a graceful Close never drops a progressing write
          // mid-flight; progress resets the deadline, so only a peer making NO
          // progress for the full `close_timeout` times out.
          match write_cancellable(&mut write_half, bytes, &mut cancel_fut, close_timeout).await {
            WriteOutcome::Aborted | WriteOutcome::TimedOut => break,
            WriteOutcome::Wrote(res) => {
              if let Err(err) = res {
                // Ignoring Err: shutdown-race reasoning.
                let _ = inbound_tx
                  .send_async(BridgeInbound::Error(BridgeError {
                    eid,
                    err,
                    received_at: Instant::now(),
                  }))
                  .await;
                break;
              }
            }
          }
        }
        Ok(BridgeOut::ShutdownWrite) => {
          if !write_closed {
            // Ignoring Err: a shutdown on a socket the peer FIN'd is
            // non-actionable; the read half observes the same condition
            // on its next read.
            let _ = write_half.shutdown().await;
            write_closed = true;
          }
        }
        Ok(BridgeOut::Close) => {
          // Ignoring Err: shutdown-race reasoning.
          let _ = inbound_tx
            .send_async(BridgeInbound::Eof(BridgeEof {
              eid,
              received_at: Instant::now(),
            }))
            .await;
          break;
        }
        Err(_) => {
          // Driver dropped the out-channel sender — treat as clean
          // close.
          // Ignoring Err: shutdown-race reasoning.
          let _ = inbound_tx
            .send_async(BridgeInbound::Eof(BridgeEof {
              eid,
              received_at: Instant::now(),
            }))
            .await;
          break;
        }
      },
      recv_msg = recv_fut => {
        // Sample `received_at` at the moment the read syscall
        // completed — this is the authoritative arrival time the FSM's
        // deadline gate compares against, so a successful response
        // queued before the exchange deadline is not retroactively
        // failed by the driver's later post-deadline `Instant::now()`.
        let received_at = Instant::now();
        let BufResult(res, buf) = recv_msg;
        match res {
          Ok(0) => {
            // Peer FIN'd its write side — its half of the exchange is
            // complete. Surface a single EOF marker so the coordinator
            // advances through `handle_transport_data(.., eof=true)`,
            // then flip to read-closed mode (keep the write side alive
            // for the local response).
            // Ignoring Err: shutdown-race reasoning.
            let _ = inbound_tx
              .send_async(BridgeInbound::Eof(BridgeEof { eid, received_at }))
              .await;
            read_closed = true;
          }
          Ok(n) => {
            // Tight copy into a right-sized Vec; the original 16 KiB
            // `buf` drops here.
            let bytes = buf[..n].to_vec();
            // Ignoring Err: shutdown-race reasoning.
            let _ = inbound_tx
              .send_async(BridgeInbound::Bytes(BridgeBytes {
                eid,
                bytes,
                received_at,
              }))
              .await;
          }
          Err(err) => {
            // Ignoring Err: shutdown-race reasoning.
            let _ = inbound_tx
              .send_async(BridgeInbound::Error(BridgeError {
                eid,
                err,
                received_at,
              }))
              .await;
            break;
          }
        }
      }
    }
  }
}

/// The result of a [`write_cancellable`] race — one of three outcomes.
enum WriteOutcome {
  /// An explicit abort preempted the write: the bridge must tear down and
  /// discard, writing nothing further.
  Aborted,
  /// The write completed (or errored); the inner result carries the I/O
  /// outcome the caller maps to a `BridgeInbound::Error` on failure. A partial
  /// write of zero bytes (no progress possible) is reported here as a
  /// [`io::ErrorKind::WriteZero`] error.
  Wrote(io::Result<()>),
  /// A single partial write made NO progress for the full `close_timeout` (a
  /// non-reading peer with no remaining cancel path): the bridge must tear
  /// down, dropping its write half so the OS RSTs the stuck stream. A peer that
  /// keeps reading — even slowly — resets the deadline on every chunk and never
  /// reaches this.
  TimedOut,
}

/// Writes `bytes` fully via a chunked loop, racing EACH partial write against
/// TWO backstops, for one of three outcomes: aborted / written / timed-out.
///
/// 1. The bridge's hoisted `cancel_fut` — resolves ONLY on an explicit
///    `StreamAction::Abort` (a graceful `StreamAction::Close` maps its
///    handle-drop disconnect to a never-resolving future before this is ever
///    called). Listed FIRST in every iteration so it preempts immediately, even
///    a write stalled mid-frame on a peer that stopped reading. On a graceful
///    close the write always wins this arm and the queued bytes flush — no write
///    is dropped mid-flight and there is no partial-write / duplication hazard.
/// 2. A `compio::time::sleep(close_timeout)` re-armed FRESH on every iteration —
///    the backstop for a post-Close drain that has NO remaining cancel path.
///    After a graceful Close the driver removed the handle, so a non-reading
///    peer could otherwise wedge the drain forever. Because the deadline resets
///    on each partial write, it is a NO-PROGRESS (idle) timeout, not a cap on
///    total write duration: a peer that keeps reading — even slowly, so the
///    whole frame takes longer than `close_timeout` to drain — advances on every
///    chunk and never trips it. It fires only when a single partial write makes
///    NO progress for the full `close_timeout` (a genuinely stalled peer), and
///    the caller then tears the bridge down (dropping the write half → RST).
///
/// The owned write buffer (`bytes`) is sliced per iteration via
/// [`IoBuf::slice`]; each partial `write` consumes the slice and returns it,
/// from which [`IntoInner`] recovers the `Vec<u8>` so the next iteration can
/// re-slice the unwritten tail — no per-chunk reallocation. A partial write of
/// zero bytes cannot make progress and is surfaced as a [`io::ErrorKind::WriteZero`]
/// write error.
async fn write_cancellable<W, C>(
  write_half: &mut W,
  mut bytes: Vec<u8>,
  mut cancel_fut: &mut C,
  close_timeout: Duration,
) -> WriteOutcome
where
  W: AsyncWrite,
  C: Future<Output = ()> + FusedFuture + Unpin,
{
  let total = bytes.len();
  let mut written = 0;
  while written < total {
    // Re-arm the deadline FRESH each iteration: progress (a non-empty partial
    // write) resets the clock, so this is an idle timeout, not a total-duration
    // cap. A slow-but-reading peer advances every chunk and never trips it.
    let timeout_fut = compio::time::sleep(close_timeout).fuse();
    pin_mut!(timeout_fut);
    let BufResult(res, slice) = select_biased! {
      // Explicit abort only (a disconnect was mapped to `pending()`). Listed
      // first so it preempts immediately, even a write blocked mid-frame on an
      // unresponsive peer, ahead of the timeout backstop.
      () = &mut cancel_fut => return WriteOutcome::Aborted,
      // Backstop: no progress on this partial write for the full
      // `close_timeout` (a non-reading peer) → abandon and RST on teardown.
      () = timeout_fut => return WriteOutcome::TimedOut,
      // Write the unwritten tail. `slice` consumes the owned buffer and is
      // returned so the next iteration recovers it via `into_inner`.
      res = write_half.write(bytes.slice(written..)).fuse() => res,
    };
    bytes = slice.into_inner();
    match res {
      // A zero-byte write makes no progress; surface it as a write error so the
      // caller reports it and tears down, rather than spinning forever.
      Ok(0) => {
        return WriteOutcome::Wrote(Err(io::Error::new(
          io::ErrorKind::WriteZero,
          "write returned zero bytes",
        )));
      }
      // Progress: advance and loop with a fresh deadline.
      Ok(n) => written += n,
      Err(e) => return WriteOutcome::Wrote(Err(e)),
    }
  }
  WriteOutcome::Wrote(Ok(()))
}

#[cfg(test)]
mod tests {
  use std::{net::SocketAddr, time::Duration};

  use compio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
  };
  use memberlist_proto::{
    Instant, RawRecords, TcpOptions, config::EndpointConfig, endpoint::Endpoint,
    streams::StreamEndpoint,
  };
  use smol_str::SmolStr;

  use super::*;

  /// Mint a real [`ExchangeId`] via a minimal in-memory `StreamEndpoint`.
  ///
  /// `accept_connection` allocates only in-memory bridge state (no I/O), and
  /// the `ExchangeId` it returns is opaque to the bridge — the bridge merely
  /// stamps it onto `BridgeInbound` events. These tests assert on the bytes
  /// the bridge writes to the wire, not on the stamped id, so any valid id
  /// suffices.
  fn fresh_eid() -> ExchangeId {
    let cfg = EndpointConfig::new(
      SmolStr::new("bridge-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    );
    let ep = Endpoint::new(cfg);
    let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      ep,
      TcpOptions::new(None),
      Box::new(|_| None),
      Box::new(|addr| *addr),
    );
    endpoint.accept_connection("127.0.0.1:1".parse().unwrap(), Instant::now())
  }

  /// Connect a loopback TCP pair, returning `(server, client)`.
  ///
  /// The bridge owns `server`; the test plays the peer through `client`,
  /// reading the bytes the bridge writes.
  async fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
      .await
      .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener local_addr");
    let client = TcpStream::connect(addr).await.expect("connect client");
    let (server, _peer_addr) = listener.accept().await.expect("accept server");
    (server, client)
  }

  /// A graceful `Close` must drain every byte already queued in `out_rx`
  /// before the bridge exits.
  ///
  /// Reproduces the exact graceful-teardown shape: `Bytes(response)` then
  /// `Close` are queued into `out_tx`, then the whole `BridgeHandle` is
  /// dropped (so `cancel_rx` disconnects with `Err`). The disconnect must
  /// NOT preempt the FIFO — the peer must receive the full response, then
  /// EOF.
  ///
  /// If the cancel arm broke on `Err(Disconnected)` (a graceful-close
  /// disconnect), it would win the bias and the bridge would break immediately,
  /// truncating the response — the client's `read_to_end` returning an empty
  /// slice. Mapping the disconnect to `pending()` is what keeps the FIFO drain
  /// intact.
  #[compio::test]
  async fn graceful_close_drains_queued_bytes_before_exit() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();

    let response = b"the-final-response-bytes".to_vec();

    // Queue the graceful teardown FIFO, then drop the handle so `cancel_rx`
    // disconnects with the queue already populated — the exact ordering the
    // driver produces on a `StreamAction::Close`.
    out_tx
      .send(BridgeOut::Bytes(response.clone()))
      .expect("queue response bytes");
    out_tx.send(BridgeOut::Close).expect("queue close");
    let handle = (out_tx, cancel_tx);

    // A long `close_timeout` so this graceful-drain test exercises the FIFO
    // drain, never the timeout backstop (the peer reads promptly here).
    let bridge = compio::runtime::spawn(bridge_task(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      64,
      Duration::from_secs(60),
    ));

    // Drop the handle: `cancel_tx` disconnects (the graceful-Close shape).
    drop(handle);

    // The peer must read the full response before EOF.
    let buf = vec![0u8; response.len()];
    let BufResult(res, got) = client.read_exact(buf).await;
    res.expect("peer reads the full response before EOF");
    assert_eq!(
      got, response,
      "graceful Close must flush the queued response bytes; a disconnect \
       must not truncate the FIFO"
    );

    // After the response the bridge processes `Close` and exits, half-
    // closing its write side: the peer then sees EOF.
    let BufResult(res, tail) = client.read_to_end(Vec::new()).await;
    let read_tail = res.expect("read tail after response");
    assert_eq!(read_tail, 0, "no bytes after the response, only EOF");
    assert!(tail.is_empty(), "no bytes after the response, only EOF");

    bridge.await.expect("bridge task exits cleanly");
  }

  /// An explicit `cancel_tx.send(())` (a FAILED exchange's `Abort`) MUST
  /// discard the bytes still queued in `out_rx` — the failed-exchange
  /// semantics: stale bytes (possibly encoded under a superseded encryption
  /// policy) are never written.
  #[compio::test]
  async fn explicit_abort_discards_queued_bytes() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();

    let stale = b"stale-bytes-that-must-not-reach-the-wire".to_vec();

    // Queue stale bytes, then fire an explicit abort. The abort must win
    // over the queued `Bytes` and break without writing them.
    out_tx
      .send(BridgeOut::Bytes(stale))
      .expect("queue stale bytes");
    cancel_tx.try_send(()).expect("signal explicit abort");
    // Keep `out_tx` alive so a disconnect cannot be confused for the abort:
    // the ONLY teardown signal here is the explicit `send(())`.
    let _out_tx_kept = out_tx;

    // A long `close_timeout`: the abort, not the backstop, must drive teardown.
    let bridge = compio::runtime::spawn(bridge_task(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      64,
      Duration::from_secs(60),
    ));

    // The peer must see EOF with NO bytes — the abort dropped the write half
    // without flushing the stale queue.
    let BufResult(res, got) = client.read_to_end(Vec::new()).await;
    let n = res.expect("read peer side to EOF");
    assert_eq!(
      n, 0,
      "explicit abort must discard queued bytes, got {n} byte(s)"
    );
    assert!(got.is_empty(), "explicit abort must discard queued bytes");

    bridge.await.expect("bridge task exits cleanly");
  }

  /// An explicit abort must preempt a write ALREADY IN FLIGHT — not just one
  /// still queued at the select boundary.
  ///
  /// The bridge is driven into a write it cannot complete: a response far
  /// larger than any plausible kernel socket buffer is queued and the peer
  /// NEVER reads, so its receive window collapses to zero and the bridge blocks
  /// inside the write. An explicit `cancel_tx.send(())` (a FAILED exchange's
  /// `StreamAction::Abort`) is then fired; the bridge must drop the in-flight
  /// write and exit PROMPTLY — the failed-teardown contract: a failed exchange
  /// aborts at once and discards, never waiting on the unresponsive peer.
  ///
  /// The bounded `timeout` is the assertion: an uncancellable in-flight write
  /// would block forever and leak the bridge task, so the timeout would elapse.
  /// Racing the write against the cancel future lets the abort drop it and the
  /// task returns at once, well inside the bound.
  #[compio::test]
  async fn explicit_abort_preempts_in_flight_write() {
    let (server, client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();

    // A response far larger than any kernel socket buffer: with the peer never
    // reading, its window collapses to zero and the write blocks once the
    // send/recv buffers fill — it cannot complete unless the peer reads.
    let response = vec![0xABu8; 16 * 1024 * 1024];

    out_tx
      .send(BridgeOut::Bytes(response))
      .expect("queue oversized response");
    // Keep `out_tx` alive: the ONLY teardown signal is the explicit abort, so a
    // disconnect can never be mistaken for it. The peer (`client`) is held but
    // NEVER read, so the write stays blocked until the abort preempts it.
    let _out_tx_kept = out_tx;
    let _client_kept = client;

    // A `close_timeout` far longer than the 500ms pre-abort wait and the 5s
    // outer bound, so the EXPLICIT ABORT — not the timeout backstop — is proven
    // to preempt the in-flight write.
    let bridge = compio::runtime::spawn(bridge_task(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      64,
      Duration::from_secs(60),
    ));

    // Let the bridge dequeue the response and block inside the write with the
    // kernel buffers full BEFORE firing the abort, so this exercises mid-write
    // preemption — not the select boundary an already-ready cancel would catch.
    compio::time::sleep(Duration::from_millis(500)).await;
    cancel_tx
      .try_send(())
      .expect("signal explicit abort mid-write");

    // The abort must preempt the in-flight write and the bridge must return
    // PROMPTLY — without the peer ever draining a byte. The bound is generous
    // yet fails fast: an uncancellable in-flight write would never return.
    compio::time::timeout(Duration::from_secs(5), bridge)
      .await
      .expect("bridge exits promptly on mid-write abort, not after the peer drains")
      .expect("bridge task exits cleanly");
  }

  /// A post-Close graceful drain whose peer STOPPED reading must be reclaimed by
  /// `close_timeout` — it has NO remaining cancel path, so without the timeout
  /// backstop the bridge's drain blocks FOREVER, leaking the detached task and
  /// its socket. A fully stalled peer makes NO progress, so the idle
  /// `close_timeout` fires and reclaims it.
  ///
  /// Shape: the peer sends its request then half-closes (the bridge enters
  /// read-closed mode), an oversized response is queued, and the whole handle is
  /// dropped — the exact `StreamAction::Close` ordering. The peer is then held
  /// but NEVER read, so its receive window collapses to zero and the drain stalls
  /// once the kernel buffers fill. With no cancel path the ONLY teardown is the
  /// `close_timeout` backstop.
  ///
  /// The outer `close_timeout * 5` bound fails fast: without the backstop the
  /// drain would block forever and trip it; with it the bridge reclaims within
  /// ~`close_timeout`. A SHORT `close_timeout` keeps the test fast.
  #[compio::test]
  async fn graceful_close_drain_bounded_by_close_timeout_when_peer_stalls() {
    let (server, mut client) = loopback_pair().await;
    let eid = fresh_eid();
    let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
    let (cancel_tx, cancel_rx) = flume::bounded::<()>(1);
    let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();

    let close_timeout = Duration::from_millis(300);

    // A response far larger than any kernel socket buffer: with the peer never
    // reading, its window collapses to zero and the drain blocks once the
    // send/recv buffers fill — it cannot complete unless the peer reads.
    let response = vec![0xCDu8; 16 * 1024 * 1024];

    let bridge = compio::runtime::spawn(bridge_task(
      server,
      eid,
      out_rx,
      cancel_rx,
      inbound_tx,
      64,
      close_timeout,
    ));

    // The peer sends its request then half-closes its write side, driving the
    // bridge into read-closed mode (the half-open state from which a graceful
    // Close drains the queued response).
    client.write_all(b"request").await.0.expect("write request");
    client
      .shutdown()
      .await
      .expect("half-close client write side");

    // Queue the oversized response and drop the whole handle: the graceful-Close
    // shape (`out_tx` and `cancel_tx` both disconnect, the response queued). No
    // cancel can ever be sent now — only `close_timeout` can reclaim the bridge.
    out_tx
      .send(BridgeOut::Bytes(response))
      .expect("queue oversized response");
    drop((out_tx, cancel_tx));

    // The peer is NEVER read: the drain stalls on a zero window. The bridge must
    // reclaim within ~`close_timeout`; the generous `close_timeout * 5` outer
    // bound fails fast — without the backstop the drain blocks forever and trips it.
    compio::time::timeout(close_timeout * 5, bridge)
      .await
      .expect("bridge reclaims within ~close_timeout, not blocking forever on a stalled drain")
      .expect("bridge task exits cleanly");
  }

  /// An [`AsyncWrite`] that models a peer reading SLOWLY but CONTINUOUSLY: each
  /// `write` first sleeps `delay` (the per-chunk read latency), then accepts at
  /// most `chunk` bytes. `accepted` records every byte taken, so the test can
  /// assert the FULL frame was delivered.
  struct SlowWriter {
    chunk: usize,
    delay: Duration,
    accepted: Vec<u8>,
  }

  impl AsyncWrite for SlowWriter {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
      compio::time::sleep(self.delay).await;
      let n = self.chunk.min(buf.buf_len());
      self.accepted.extend_from_slice(&buf.as_init()[..n]);
      BufResult(Ok(n), buf)
    }

    async fn flush(&mut self) -> io::Result<()> {
      Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
      Ok(())
    }
  }

  /// An [`AsyncWrite`] modelling a peer that STOPPED reading: every `write`
  /// parks far past any test `close_timeout`, making zero progress.
  struct StalledWriter;

  impl AsyncWrite for StalledWriter {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
      compio::time::sleep(Duration::from_secs(3600)).await;
      BufResult(Ok(0), buf)
    }

    async fn flush(&mut self) -> io::Result<()> {
      Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
      Ok(())
    }
  }

  /// A never-resolving cancel future: models a graceful Close, whose handle-drop
  /// disconnect is mapped to `pending()` — only an explicit abort resolves it.
  fn never_cancels() -> impl FusedFuture<Output = ()> + Unpin {
    Box::pin(futures_util::future::pending::<()>().fuse())
  }

  /// `close_timeout` is a NO-PROGRESS (idle) bound, not a cap on total write
  /// duration: a frame whose drain outlasts `close_timeout` overall must still be
  /// written IN FULL as long as each partial write makes progress within the
  /// bound. A total-duration cap would instead race the ENTIRE write against a
  /// single `sleep(close_timeout)`, lose that race mid-frame on a slow reader,
  /// and return `TimedOut` — truncating a healthy push/pull after only the first
  /// chunk.
  ///
  /// The peer accepts the frame in small chunks, each after a `delay` short of
  /// `close_timeout`, so the TOTAL drain (many chunks) runs several times longer
  /// than `close_timeout` while no single partial write ever idles that long. The
  /// drain must therefore complete `Wrote(Ok)` with the whole frame accepted.
  #[compio::test]
  async fn slow_but_progressing_reader_is_not_timed_out() {
    // SHORT idle bound; each partial write (below) completes well inside it.
    let close_timeout = Duration::from_millis(200);

    // A frame whose drain spans many chunks: 64 KiB chunks, each delayed 20ms.
    // With 1 MiB total that is sixteen chunks, ~0.32s overall — well past the
    // 200ms idle bound — yet every partial write returns in ~20ms.
    let frame: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    let mut writer = SlowWriter {
      chunk: 64 * 1024,
      delay: Duration::from_millis(20),
      accepted: Vec::new(),
    };
    let mut cancel = never_cancels();

    let start = std::time::Instant::now();
    let outcome = write_cancellable(&mut writer, frame.clone(), &mut cancel, close_timeout).await;
    let elapsed = start.elapsed();

    // The drain genuinely outlasted the idle bound — proving this is not a case
    // the bound never had a chance to fire.
    assert!(
      elapsed > close_timeout,
      "the total drain ({elapsed:?}) must exceed close_timeout ({close_timeout:?}) \
       for this to exercise the idle-vs-total-cap distinction"
    );
    assert!(
      matches!(outcome, WriteOutcome::Wrote(Ok(()))),
      "a slow-but-progressing peer must be written in full, not timed out"
    );
    assert_eq!(
      writer.accepted, frame,
      "the peer must receive the exact frame, never a truncated body"
    );
  }

  /// The idle bound still reclaims a GENUINELY stalled peer: a single partial
  /// write that makes no progress for the full `close_timeout` returns
  /// `TimedOut`, so the bridge tears down (drop write half → RST). This is the
  /// stalled-peer backstop the slow-reader fix must not regress.
  #[compio::test]
  async fn stalled_peer_still_times_out() {
    let close_timeout = Duration::from_millis(150);
    let mut writer = StalledWriter;
    let mut cancel = never_cancels();

    let outcome =
      write_cancellable(&mut writer, vec![0xAAu8; 4096], &mut cancel, close_timeout).await;

    assert!(
      matches!(outcome, WriteOutcome::TimedOut),
      "a peer making no progress for close_timeout must time out"
    );
  }

  /// An explicit abort preempts even a write blocked mid-frame on an
  /// unresponsive peer: the cancel future resolving wins ahead of both the
  /// stalled write and the idle timeout, returning `Aborted` at once. This is
  /// the failed-exchange fast-path the chunked loop must preserve.
  #[compio::test]
  async fn explicit_abort_preempts_stalled_write() {
    // A long idle bound so the abort — not the timeout — is proven to preempt.
    let close_timeout = Duration::from_secs(60);
    let mut writer = StalledWriter;
    // An already-resolved cancel future: the explicit-abort signal.
    let mut cancel = Box::pin(futures_util::future::ready(()).fuse());

    let outcome =
      write_cancellable(&mut writer, vec![0xBBu8; 4096], &mut cancel, close_timeout).await;

    assert!(
      matches!(outcome, WriteOutcome::Aborted),
      "an explicit abort must preempt a stalled write immediately"
    );
  }
}
