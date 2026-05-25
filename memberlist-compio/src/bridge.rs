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
//! to the peer before the close signal fires. A dual-channel design
//! (separate transmit + control senders) would let `select!` resolve the
//! close arm first when both are ready and orphan in-flight bytes.
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

use std::time::Instant;

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt, util::Splittable},
};
use flume::{Receiver, Sender};
use futures_util::{FutureExt, pin_mut, select};

use crate::driver::{BridgeBytes, BridgeEof, BridgeError, BridgeInbound, BridgeOut, ExchangeId};

// Per-bridge recv buffer size is now configured by the driver via
// [`crate::DriverOptions::with_bridge_recv_buf_len`]; the historical
// default value lives at
// [`crate::DEFAULT_BRIDGE_RECV_BUF_LEN`].

/// Run the per-bridge byte-mover loop until a [`BridgeOut::Close`] arrives,
/// the driver drops the out-channel sender, or the stream returns an I/O
/// error.
///
/// Ownership transfer:
/// - `stream`: the post-handshake stream value (TcpStream or
///   `Split<TlsStream>`).
/// - `eid`: the coordinator-allocated handle for tagging inbound events.
/// - `out_rx`: the driver's bytes-or-control FIFO into this bridge.
/// - `inbound_tx`: bytes / eof / error events the driver consumes.
///
/// All `Sender::send_async` failures are ignored with a justification: if
/// the driver dropped the inbound receiver we are already shutting down
/// and the event can be discarded.
pub(crate) async fn bridge_task<S>(
  stream: S,
  eid: ExchangeId,
  out_rx: Receiver<BridgeOut>,
  inbound_tx: Sender<BridgeInbound>,
  recv_buf_len: usize,
) where
  S: Splittable,
  S::ReadHalf: AsyncRead,
  S::WriteHalf: AsyncWrite,
{
  let (mut read_half, mut write_half) = stream.split();
  let mut write_closed = false;
  let mut read_closed = false;

  loop {
    if read_closed {
      // Read-closed mode: peer FIN already observed and the matching
      // EOF marker already sent to the driver. The bridge stays alive
      // to accept any late `Bytes` the coordinator surfaces (the
      // server side writes its response AFTER the request EOF).
      // Exits on `Close`, sender drop, or the next write error.
      match out_rx.recv_async().await {
        Ok(BridgeOut::Bytes(bytes)) => {
          if write_closed {
            // Discard bytes after a half-close — the machine guarantees
            // it stops queueing for this exchange after `ShutdownWrite`,
            // but the gate is defensive against any in-flight chunk
            // that raced past the channel boundary.
            continue;
          }
          let BufResult(res, _buf) = write_half.write_all(bytes).await;
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
        Ok(BridgeOut::ShutdownWrite) => {
          if !write_closed {
            // Ignoring Err: a shutdown on a socket the peer FIN'd is
            // non-actionable.
            let _ = write_half.shutdown().await;
            write_closed = true;
          }
        }
        Ok(BridgeOut::Close) | Err(_) => {
          // No EOF send here — the read-EOF transition in the
          // both-halves-live arm already sent one. Just exit.
          break;
        }
      }
      continue;
    }

    // Both-halves-live mode: select between out_rx and read.
    let buf = vec![0u8; recv_buf_len];
    let recv_fut = read_half.read(buf).fuse();
    let out_fut = out_rx.recv_async();
    pin_mut!(recv_fut, out_fut);

    select! {
      out_msg = out_fut => match out_msg {
        Ok(BridgeOut::Bytes(bytes)) => {
          if write_closed {
            continue;
          }
          let BufResult(res, _buf) = write_half.write_all(bytes).await;
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
