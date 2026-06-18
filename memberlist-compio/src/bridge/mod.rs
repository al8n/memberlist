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
//! `Err(Canceled)`. That cancellation must not preempt the FIFO — it
//! winning the bias over already-queued `Bytes`/`Close`, or
//! dropping a write mid-flight, would truncate a clean exchange's final
//! response. So the cancel future maps that `Err` to a future that
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
//!
//! No-data-loss depends on the exchanges being strictly HALF-DUPLEX. Dropping a
//! recv that already completed in the kernel (its CQE consumed bytes off the
//! socket) would lose those bytes — but every exchange kind here is request-then-
//! response, so a recv is only in flight while our out channel is silent, and the
//! late out-messages for a live exchange are `Close` (delivered in read-closed
//! mode, where no recv is in flight) or `Abort` (the exchange already failed, so
//! loss is moot). If a FULL-DUPLEX exchange kind is ever added, its recv must
//! move to a persistent (never-dropped) future like the hoisted accept future,
//! or a completed-but-dropped read will silently lose mid-stream bytes.

use std::{future::Future, io, time::Duration};

use memberlist_proto::Instant;

use compio::{
  buf::{BufResult, IntoInner, IoBuf},
  io::{AsyncRead, AsyncWrite, util::Splittable},
};
use flume::{Receiver, Sender};
use futures_util::{FutureExt, future::FusedFuture, pin_mut, select_biased};

use crate::{
  driver::{BridgeBytes, BridgeEof, BridgeError, BridgeInbound, BridgeOut},
  driver_shared::ExchangeId,
};

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
///   resolves the cancel receiver with `Err(Canceled)` instead; that is NOT
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
  cancel_rx: futures_channel::oneshot::Receiver<()>,
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
  // `StreamAction::Abort` for a FAILED exchange — resolves the cancel
  // receiver with `Ok(())`, which both wins the select bias at the boundary
  // AND preempts a write already in flight (the write arms race against this
  // same future). A graceful `StreamAction::Close` instead drops the
  // `BridgeHandle` (and thus `cancel_tx`) AFTER queuing `BridgeOut::Close`
  // behind the response `Bytes`, which resolves the cancel receiver with
  // `Err(Canceled)`; that cancellation is NOT an abort, so it is mapped
  // to a future that NEVER resolves. Mapping that `Err` to `pending()`
  // means the cancel future can never win a write race on a graceful close
  // (no write is dropped mid-flight — no partial-write / duplication
  // hazard); the graceful drain+teardown is driven SOLELY by `out_rx`'s
  // FIFO (`Bytes` then `Close`/`Err`). Fusing keeps the future
  // `is_terminated()`-safe across the many `select_biased!` re-polls.
  let cancel_fut = async {
    match cancel_rx.await {
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
              WriteStatus::Aborted | WriteStatus::TimedOut => break,
              WriteStatus::Wrote(res) => {
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
            WriteStatus::Aborted | WriteStatus::TimedOut => break,
            WriteStatus::Wrote(res) => {
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
enum WriteStatus {
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
) -> WriteStatus
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
      () = &mut cancel_fut => return WriteStatus::Aborted,
      // Backstop: no progress on this partial write for the full
      // `close_timeout` (a non-reading peer) → abandon and RST on teardown.
      () = timeout_fut => return WriteStatus::TimedOut,
      // Write the unwritten tail. `slice` consumes the owned buffer and is
      // returned so the next iteration recovers it via `into_inner`.
      res = write_half.write(bytes.slice(written..)).fuse() => res,
    };
    bytes = slice.into_inner();
    match res {
      // A zero-byte write makes no progress; surface it as a write error so the
      // caller reports it and tears down, rather than spinning forever.
      Ok(0) => {
        return WriteStatus::Wrote(Err(io::Error::new(
          io::ErrorKind::WriteZero,
          "write returned zero bytes",
        )));
      }
      // Progress: advance and loop with a fresh deadline.
      Ok(n) => written += n,
      Err(e) => return WriteStatus::Wrote(Err(e)),
    }
  }
  WriteStatus::Wrote(Ok(()))
}

#[cfg(all(test, feature = "tcp"))]
mod tests;
