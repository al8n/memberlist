use std::{net::SocketAddr, time::Duration};

use crate::StreamEndpoint;
use compio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
};
use memberlist_proto::{
  Instant, RawRecords, config::EndpointOptions, endpoint::Endpoint, streams::LabelOptions,
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
  let cfg = EndpointOptions::new(
    SmolStr::new("bridge-test"),
    "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
  );
  let ep = Endpoint::new(cfg, crate::gossip_rng().expect("test: OS entropy"));
  let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|addr| *addr),
  );
  endpoint
    .accept_connection("127.0.0.1:1".parse().unwrap(), Instant::now())
    .expect("test: connection admitted")
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
/// dropped (so `cancel_rx` resolves with `Err`). That cancellation must
/// NOT preempt the FIFO — the peer must receive the full response, then
/// EOF.
///
/// If the cancel arm broke on `Err(Canceled)` (a graceful-close
/// sender drop), it would win the bias and the bridge would break immediately,
/// truncating the response — the client's `read_to_end` returning an empty
/// slice. Mapping that `Err` to `pending()` is what keeps the FIFO drain
/// intact.
#[compio::test]
async fn graceful_close_drains_queued_bytes_before_exit() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
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
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
  let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();

  let stale = b"stale-bytes-that-must-not-reach-the-wire".to_vec();

  // Queue stale bytes, then fire an explicit abort. The abort must win
  // over the queued `Bytes` and break without writing them.
  out_tx
    .send(BridgeOut::Bytes(stale))
    .expect("queue stale bytes");
  cancel_tx.send(()).expect("signal explicit abort");
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
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
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
  cancel_tx.send(()).expect("signal explicit abort mid-write");

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
// Windows accepts the oversized response in slow chunks rather than collapsing
// the peer's receive window to zero, so the no-progress stall this asserts never
// forms and the idle `close_timeout` never trips within the bound. The drain is
// still reclaimed (the write eventually completes), just not via the idle path
// this exercises — so the scenario is Linux/macOS-specific.
#[cfg_attr(
  windows,
  ignore = "Windows buffers the oversized response in chunks; the zero-window stall never forms"
)]
#[compio::test]
async fn graceful_close_drain_bounded_by_close_timeout_when_peer_stalls() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
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
  async fn write<T>(&mut self, buf: T) -> BufResult<usize, T>
  where
    T: IoBuf,
  {
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
  async fn write<T>(&mut self, buf: T) -> BufResult<usize, T>
  where
    T: IoBuf,
  {
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

/// The both-halves-live recv arm forwards peer bytes to the driver as
/// `BridgeInbound::Bytes` carrying the same `eid`. This exercises the recv
/// `Ok(n)` data path that the abort / graceful-close tests never reach (they
/// only drive `out_rx`). Teardown is via an explicit abort (the proven
/// deterministic signal), so the assertion is solely the forwarded bytes.
#[compio::test]
async fn recv_forwards_peer_bytes_to_driver() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
  let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
  // Keep the out-channel alive so the ONLY teardown is the explicit abort.
  let _out_tx_kept = out_tx;

  let bridge = compio::runtime::spawn(bridge_task(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    64,
    Duration::from_secs(60),
  ));

  // The peer writes a request; the bridge's recv `Ok(n)` arm forwards it.
  let request = b"hello-from-peer".to_vec();
  client
    .write_all(request.clone())
    .await
    .0
    .expect("peer writes request");

  // The forwarded bytes carry this bridge's eid and match what was sent.
  let first = inbound_rx
    .recv_async()
    .await
    .expect("bridge forwards the request bytes");
  match first {
    BridgeInbound::Bytes(BridgeBytes {
      eid: got_eid,
      bytes,
      ..
    }) => {
      assert_eq!(got_eid, eid, "forwarded bytes carry the bridge's eid");
      assert_eq!(bytes, request, "forwarded bytes match what the peer sent");
    }
    BridgeInbound::Eof(_) => panic!("expected Bytes, got Eof"),
    BridgeInbound::Error(_) => panic!("expected Bytes, got Error"),
  }

  // Tear down deterministically via the explicit abort (proven signal).
  cancel_tx.send(()).expect("signal explicit abort");
  bridge.await.expect("bridge exits on abort");
}

/// A peer FIN surfaces a single `BridgeInbound::Eof` and flips the bridge into
/// read-closed mode, after which a LATE response queued on `out_rx` (the
/// inbound-server side writes its push/pull response AFTER the request EOF) is
/// still written to the peer, then a trailing `Close` tears the bridge down.
/// This exercises the recv `Ok(0)` EOF transition, the read-closed-mode `Bytes`
/// write arm, and the read-closed `Close` exit arm — none reached by the
/// both-halves-live abort / graceful-close tests.
#[compio::test]
async fn read_closed_mode_writes_late_response_then_closes() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (_cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
  let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();

  let bridge = compio::runtime::spawn(bridge_task(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    64,
    Duration::from_secs(60),
  ));

  // Peer sends its request and half-closes: the bridge forwards the bytes,
  // then observes the FIN (recv `Ok(0)`) and enters read-closed mode.
  client
    .write_all(b"req")
    .await
    .0
    .expect("peer writes request");
  client.shutdown().await.expect("peer half-closes");

  // Drain the request bytes, then assert the EOF marker (the `Ok(0)` arm).
  let _bytes = inbound_rx.recv_async().await.expect("request bytes");
  let eof = inbound_rx
    .recv_async()
    .await
    .expect("eof after the peer FIN");
  match eof {
    BridgeInbound::Eof(BridgeEof { eid: got_eid, .. }) => {
      assert_eq!(got_eid, eid, "EOF carries the bridge's eid");
    }
    BridgeInbound::Bytes(_) => panic!("expected Eof after the request, got more Bytes"),
    BridgeInbound::Error(_) => panic!("expected Eof after the request, got Error"),
  }

  // The server-side response arrives AFTER the request EOF — exactly the
  // ordering that requires the bridge to stay alive in read-closed mode.
  let response = b"server-response-after-eof".to_vec();
  out_tx
    .send(BridgeOut::Bytes(response.clone()))
    .expect("queue late response");
  out_tx.send(BridgeOut::Close).expect("queue close");

  // The peer reads the full late response, then EOF.
  let buf = vec![0u8; response.len()];
  let BufResult(res, got) = client.read_exact(buf).await;
  res.expect("peer reads the late response in read-closed mode");
  assert_eq!(got, response, "the late response reaches the peer");

  bridge
    .await
    .expect("bridge exits after the read-closed Close");
}

/// A `ShutdownWrite` half-closes the bridge's write side so the peer's read
/// side observes FIN while the bridge keeps reading. This is the push/pull
/// requester's half-close anchor; the abort / graceful-close tests never
/// drive the `ShutdownWrite` arm. Teardown is via an explicit abort.
#[compio::test]
async fn shutdown_write_half_closes_peer_read_side() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
  let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();

  let bridge = compio::runtime::spawn(bridge_task(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    64,
    Duration::from_secs(60),
  ));

  // Write the push, then half-close the write side (the requester's anchor).
  let push = b"push-request".to_vec();
  out_tx
    .send(BridgeOut::Bytes(push.clone()))
    .expect("queue push bytes");
  out_tx
    .send(BridgeOut::ShutdownWrite)
    .expect("queue shutdown-write");

  // The peer reads the push, then sees FIN on its read side: `read_to_end`
  // returns exactly the push bytes (the bridge half-closed only its write
  // side, so the FIN follows the push).
  let BufResult(res, got) = client.read_to_end(Vec::new()).await;
  let n = res.expect("peer reads the push then sees FIN");
  assert_eq!(n, push.len(), "peer received the full push before FIN");
  assert_eq!(got, push, "peer received exactly the push bytes");

  // The bridge's read half is still open (only the write side half-closed);
  // tear down deterministically via the explicit abort.
  cancel_tx.send(()).expect("signal explicit abort");
  bridge.await.expect("bridge exits on abort");
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
