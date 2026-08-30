use super::*;

use std::{cell::RefCell, collections::VecDeque, rc::Rc, vec, vec::Vec};

use core::net::{IpAddr, Ipv4Addr};
use smoltcp::{
  iface::{Config as IfConfig, Interface, SocketSet},
  phy::{ChecksumCapabilities, Device, DeviceCapabilities, Medium, RxToken, TxToken},
  time::{Duration as SmolDuration, Instant as SmolInstant},
  wire::{HardwareAddress, IpAddress, IpCidr},
};

/// A shared in-memory frame FIFO.
type Wire = Rc<RefCell<VecDeque<Vec<u8>>>>;

/// One end of a loopback `Medium::Ip` link: reads from `rx`, writes to `tx`.
struct LoopDevice {
  rx: Wire,
  tx: Wire,
}

/// Cross-wire two `LoopDevice`s so each side's TX is the other's RX.
fn link() -> (LoopDevice, LoopDevice) {
  let a2b: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let b2a: Wire = Rc::new(RefCell::new(VecDeque::new()));
  (
    LoopDevice {
      rx: b2a.clone(),
      tx: a2b.clone(),
    },
    LoopDevice { rx: a2b, tx: b2a },
  )
}

struct LRx(Vec<u8>);
struct LTx(Wire);

impl RxToken for LRx {
  fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
    f(&self.0)
  }
}

impl TxToken for LTx {
  fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
    let mut buf = vec![0u8; len];
    let r = f(&mut buf);
    self.0.borrow_mut().push_back(buf);
    r
  }
}

impl Device for LoopDevice {
  type RxToken<'a> = LRx;
  type TxToken<'a> = LTx;

  fn receive(&mut self, _t: SmolInstant) -> Option<(LRx, LTx)> {
    let frame = self.rx.borrow_mut().pop_front()?;
    Some((LRx(frame), LTx(self.tx.clone())))
  }

  fn transmit(&mut self, _t: SmolInstant) -> Option<LTx> {
    Some(LTx(self.tx.clone()))
  }

  fn capabilities(&self) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.medium = Medium::Ip;
    caps.max_transmission_unit = 1500;
    caps.checksum = ChecksumCapabilities::ignored();
    caps
  }
}

/// Build a `Medium::Ip` interface at `10.0.0.{octet}/24` over `device`.
fn iface(device: &mut LoopDevice, octet: u8) -> Interface {
  let mut cfg = IfConfig::new(HardwareAddress::Ip);
  cfg.random_seed = octet as u64;
  let mut iface = Interface::new(cfg, device, SmolInstant::from_millis(0));
  iface.update_ip_addrs(|addrs| {
    addrs
      .push(IpCidr::new(IpAddress::v4(10, 0, 0, octet), 24))
      .expect("push ip");
  });
  iface
}

/// Establish one TCP connection between two loopback interfaces and return the
/// handles plus the per-node `(iface, socket-set, device)` so a test can drive
/// either side through the [`SmoltcpStream`] view.
///
/// `a` is the active opener (dials `b`); `b` is the passive listener. Returns
/// `(ha, hb)` — the socket handles — leaving both sockets Established.
#[allow(clippy::type_complexity)]
fn established() -> (
  (Interface, SocketSet<'static>, LoopDevice, SocketHandle),
  (Interface, SocketSet<'static>, LoopDevice, SocketHandle),
) {
  let (mut dev_a, mut dev_b) = link();
  let mut if_a = iface(&mut dev_a, 1);
  let mut if_b = iface(&mut dev_b, 2);
  let mut set_a = SocketSet::new(Vec::new());
  let mut set_b = SocketSet::new(Vec::new());

  let mk = || {
    tcp::Socket::new(
      tcp::SocketBuffer::new(vec![0u8; 4096]),
      tcp::SocketBuffer::new(vec![0u8; 4096]),
    )
  };
  let ha = set_a.add(mk());
  let hb = set_b.add(mk());

  let local_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7946);
  let remote_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946);

  // B listens; A dials. Both views borrow their own set for the call only.
  {
    let cell_b = RefCell::new(&mut set_b);
    let mut sb = SmoltcpStream::new(&mut if_b, &cell_b);
    sb.listen(hb, 7946, SlotGen::START).expect("listen");
  }
  {
    let cell_a = RefCell::new(&mut set_a);
    let mut sa = SmoltcpStream::new(&mut if_a, &cell_a);
    sa.connect(ha, remote_b, local_a.port(), SlotGen::START)
      .expect("connect");
  }

  // Pump both stacks until A is send-capable (the handshake settled).
  for t in 0..50u64 {
    let now = SmolInstant::from_millis(t as i64);
    if_a.poll(now, &mut dev_a, &mut set_a);
    if_b.poll(now, &mut dev_b, &mut set_b);
    if set_a.get::<tcp::Socket>(ha).may_send() && set_b.get::<tcp::Socket>(hb).may_send() {
      break;
    }
  }
  assert!(
    set_a.get::<tcp::Socket>(ha).may_send(),
    "handshake did not complete (A not send-capable)"
  );

  ((if_a, set_a, dev_a, ha), (if_b, set_b, dev_b, hb))
}

/// Pump both stacks a few ticks so in-flight segments (a FIN, an RST, an ACK)
/// are delivered and the receiving socket's state machine advances.
fn settle(
  a: &mut (Interface, SocketSet<'static>, LoopDevice, SocketHandle),
  b: &mut (Interface, SocketSet<'static>, LoopDevice, SocketHandle),
  start_ms: u64,
) {
  for t in start_ms..start_ms + 20 {
    let now = SmolInstant::from_millis(t as i64);
    a.0.poll(now, &mut a.2, &mut a.1);
    b.0.poll(now, &mut b.2, &mut b.1);
  }
}

/// `recv_finished` of the `SmoltcpStream` view over `node`'s socket.
fn recv_finished(node: &mut (Interface, SocketSet<'static>, LoopDevice, SocketHandle)) -> bool {
  let cell = RefCell::new(&mut node.1);
  let view = SmoltcpStream::new(&mut node.0, &cell);
  view.recv_finished(node.3)
}

/// `teardown_done` of the `SmoltcpStream` view over `node`'s socket for gen `g`.
fn teardown_done(
  node: &mut (Interface, SocketSet<'static>, LoopDevice, SocketHandle),
  g: SlotGen,
) -> bool {
  let cell = RefCell::new(&mut node.1);
  let view = SmoltcpStream::new(&mut node.0, &cell);
  view.teardown_done(node.3, g)
}

/// #161: after `abort`, the handle is NOT reusable (`teardown_done` reports
/// `false`) until a stack poll has dispatched the RST — so a same-pump
/// reallocation cannot suppress the pending reset by re-`listen`/`connect`ing the
/// handle (which `reset()`s the socket and drops the queued RST). Once the RST has
/// egressed (the remote tuple is cleared) the handle becomes reusable.
///
/// Mutation anchor: weaken the gate to `!is_open()` (dropping the RST-pending
/// term) and the pre-poll assertion below flips — a same-pump reuse would then be
/// allowed while the RST is still queued.
#[test]
fn abort_then_immediate_reallocation_waits_for_rst_egress() {
  let (mut a, _b) = established();

  // A aborts its established socket: smoltcp sets `Closed` but KEEPS the remote
  // tuple until the next dispatch emits the single RST.
  {
    let cell = RefCell::new(&mut a.1);
    let mut view = SmoltcpStream::new(&mut a.0, &cell);
    view.abort(a.3, SlotGen::START);
  }
  assert_eq!(
    a.1.get::<tcp::Socket>(a.3).state(),
    tcp::State::Closed,
    "abort moves the socket to Closed"
  );
  assert!(
    a.1.get::<tcp::Socket>(a.3).remote_endpoint().is_some(),
    "the aborted socket keeps its remote tuple until the RST egresses"
  );
  assert!(
    !teardown_done(&mut a, SlotGen::START),
    "an aborted handle is NOT reusable until its RST has egressed (#161)"
  );

  // Poll A's stack until the RST is dispatched and the tuple cleared.
  for i in 0..20i64 {
    a.0
      .poll(SmolInstant::from_millis(100 + i), &mut a.2, &mut a.1);
    if a.1.get::<tcp::Socket>(a.3).remote_endpoint().is_none() {
      break;
    }
  }
  assert!(
    a.1.get::<tcp::Socket>(a.3).remote_endpoint().is_none(),
    "the RST egress clears the remote tuple"
  );
  assert!(
    teardown_done(&mut a, SlotGen::START),
    "once the RST has egressed the handle is reusable"
  );
}

/// Control: a cleanly-closed socket — both FINs exchanged, no RST pending — IS
/// reusable at once. The reuse gate withholds only a socket whose abort RST has not
/// yet egressed, never a graceful close that reached TimeWait / Closed.
#[test]
fn cleanly_closed_socket_is_reusable() {
  let (mut a, mut b) = established();

  // Both sides close their write halves gracefully, then settle the FIN handshake.
  {
    let cell = RefCell::new(&mut a.1);
    let mut view = SmoltcpStream::new(&mut a.0, &cell);
    view.close(a.3, SlotGen::START);
  }
  {
    let cell = RefCell::new(&mut b.1);
    let mut view = SmoltcpStream::new(&mut b.0, &cell);
    view.close(b.3, SlotGen::START);
  }
  settle(&mut a, &mut b, 100);

  assert!(
    !a.1.get::<tcp::Socket>(a.3).is_open(),
    "a cleanly-closed socket has left the open states"
  );
  assert!(
    teardown_done(&mut a, SlotGen::START),
    "a cleanly-closed socket (no RST pending) is reusable at once"
  );
}

/// A graceful peer FIN is reported as a clean EOF: after B `close()`s its write
/// half, A drains the bytes and observes `recv_finished == true`.
#[test]
fn graceful_fin_is_reported_as_eof() {
  let (mut a, mut b) = established();

  // B sends a payload then gracefully closes its write half (FIN after the data).
  b.1
    .get_mut::<tcp::Socket>(b.3)
    .send_slice(b"hello")
    .expect("send");
  b.1.get_mut::<tcp::Socket>(b.3).close();
  settle(&mut a, &mut b, 100);

  // Before draining, the rx ring still holds "hello", so no premature EOF.
  assert!(
    !recv_finished(&mut a),
    "EOF must not be reported while buffered bytes remain"
  );
  // Drain the payload.
  let mut buf = [0u8; 16];
  let n = a
    .1
    .get_mut::<tcp::Socket>(a.3)
    .recv_slice(&mut buf)
    .expect("recv");
  assert_eq!(&buf[..n], b"hello");

  // With the data drained and the peer FIN received, the EOF is now reported.
  // A is in CloseWait (it has not yet closed its own write half).
  assert_eq!(a.1.get::<tcp::Socket>(a.3).state(), tcp::State::CloseWait);
  assert!(
    recv_finished(&mut a),
    "a drained graceful FIN must report recv_finished == true (clean EOF)"
  );
}

/// A connection RESET (RST) is NOT a clean EOF. After B `abort()`s (sending an
/// RST), A's socket reaches `Closed` WITHOUT `rx_fin_received`, so the pre-refactor
/// `recv_slice` would return `InvalidState` — `recv_finished` must therefore report
/// `false`, surfacing the reset as a failure rather than a graceful completion.
#[test]
fn reset_is_not_reported_as_eof() {
  let (mut a, mut b) = established();

  // B aborts: smoltcp emits an RST and moves B's socket straight to Closed.
  b.1.get_mut::<tcp::Socket>(b.3).abort();
  settle(&mut a, &mut b, 100);

  // The RST drives A to Closed (no FIN handshake, no rx_fin_received).
  assert_eq!(
    a.1.get::<tcp::Socket>(a.3).state(),
    tcp::State::Closed,
    "a received RST must drive the peer socket to Closed"
  );
  assert!(
    !recv_finished(&mut a),
    "a reset Closed socket must report recv_finished == false (a reset is a \
       failure, not a graceful EOF) — mapping it to true would silently complete \
       an aborted exchange"
  );
}

/// A RST that arrives mid-stream (the peer aborts after sending some bytes, the
/// classic abrupt teardown) is likewise not a clean EOF: the buffered bytes are
/// dropped by the reset and `recv_finished` stays `false`.
#[test]
fn mid_stream_reset_is_not_reported_as_eof() {
  let (mut a, mut b) = established();

  // B sends a payload, then immediately aborts (RST) before A drains it.
  b.1
    .get_mut::<tcp::Socket>(b.3)
    .send_slice(b"partial")
    .expect("send");
  settle(&mut a, &mut b, 100);
  b.1.get_mut::<tcp::Socket>(b.3).abort();
  settle(&mut a, &mut b, 200);

  // The RST resets A's socket: it is Closed and its rx buffer was cleared, so
  // there is no orderly EOF to report.
  assert_eq!(a.1.get::<tcp::Socket>(a.3).state(), tcp::State::Closed);
  assert!(
    !recv_finished(&mut a),
    "a mid-stream reset must not be reported as a graceful EOF"
  );
}

/// Build a fresh 4 KiB `tcp::Socket` carrying the finite inactivity timeout the
/// driver installs at socket creation (`Options::close_timeout` → smoltcp
/// `Duration`), the mechanism that reaps a stalled reliable socket.
fn timed_socket(timeout_ms: u64) -> tcp::Socket<'static> {
  let mut sock = tcp::Socket::new(
    tcp::SocketBuffer::new(vec![0u8; 4096]),
    tcp::SocketBuffer::new(vec![0u8; 4096]),
  );
  sock.set_timeout(Some(SmolDuration::from_millis(timeout_ms)));
  sock
}

/// ML-LIFE-01: a single unauthenticated half-open (a peer SYN with the final ACK
/// withheld) must NOT pin the direct-smoltcp listener forever. With the socket
/// inactivity timeout installed, the parked `SynReceived` is reaped to `Closed`,
/// and re-`listen` (what `Memberlist::poll` does) frees the slot to accept again —
/// bounding the DoS to one timeout window instead of a permanent black-hole.
///
/// Mutation anchor: delete the `set_timeout` inside `timed_socket` and the socket
/// stays `SynReceived` forever, so the `Closed` assertion below fails.
#[test]
fn half_open_listener_is_reaped_by_inactivity_timeout() {
  const TIMEOUT_MS: i64 = 200;
  let (mut dev_a, mut dev_b) = link();
  let mut if_a = iface(&mut dev_a, 1);
  let mut if_b = iface(&mut dev_b, 2);
  let mut set_a = SocketSet::new(Vec::new());
  let mut set_b = SocketSet::new(Vec::new());

  // B is the listener carrying the inactivity timeout.
  let hb = set_b.add(timed_socket(TIMEOUT_MS as u64));
  set_b
    .get_mut::<tcp::Socket>(hb)
    .listen(7946)
    .expect("listen");

  // A dials B, emitting a SYN. We deliver A's SYN to B (B: Listen → SynReceived,
  // which sends a SYN-ACK) but then NEVER poll A again, so A never sees the
  // SYN-ACK and never sends the final ACK — precisely the withheld-ACK attack.
  let ha = set_a.add(timed_socket(TIMEOUT_MS as u64));
  let remote_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946);
  set_a
    .get_mut::<tcp::Socket>(ha)
    .connect(if_a.context(), to_endpoint(remote_b), 49_000u16)
    .expect("connect");

  if_a.poll(SmolInstant::from_millis(0), &mut dev_a, &mut set_a);
  if_b.poll(SmolInstant::from_millis(0), &mut dev_b, &mut set_b);
  assert_eq!(
    set_b.get::<tcp::Socket>(hb).state(),
    tcp::State::SynReceived,
    "the withheld-ACK peer must park B half-open"
  );

  // Advance B's clock past the inactivity timeout, never delivering A's ACK (A is
  // never polled again). B's dispatch observes the timeout and aborts to Closed.
  let mut t = 1i64;
  while t < TIMEOUT_MS + 500 {
    if_b.poll(SmolInstant::from_millis(t), &mut dev_b, &mut set_b);
    if set_b.get::<tcp::Socket>(hb).state() == tcp::State::Closed {
      break;
    }
    t += 10;
  }
  assert_eq!(
    set_b.get::<tcp::Socket>(hb).state(),
    tcp::State::Closed,
    "the inactivity timeout must reap the half-open listener to Closed"
  );

  // The reaped socket is free again: re-`listen` restores it IN PLACE (the same
  // handle the engine keeps as its listener), and the timeout survives the reset
  // so the next half-open is reaped too.
  set_b
    .get_mut::<tcp::Socket>(hb)
    .listen(7946)
    .expect("re-listen");
  assert!(
    set_b.get::<tcp::Socket>(hb).is_listening(),
    "a reaped listener must re-listen so the node accepts inbound streams again"
  );
  assert_eq!(
    set_b.get::<tcp::Socket>(hb).timeout(),
    Some(SmolDuration::from_millis(TIMEOUT_MS as u64)),
    "the inactivity timeout must survive the reap + re-listen (smoltcp reset() \
     preserves it) so a re-armed listener stays protected"
  );
}

/// Control: the socket timeout is an INACTIVITY timeout, not a hard cap on a live
/// exchange. An established pair that keeps exchanging data across a span far
/// longer than the timeout is never aborted, because each delivered packet resets
/// the inactivity clock.
#[test]
fn active_exchange_within_window_not_reaped() {
  const TIMEOUT_MS: i64 = 200;
  let (mut dev_a, mut dev_b) = link();
  let mut if_a = iface(&mut dev_a, 1);
  let mut if_b = iface(&mut dev_b, 2);
  let mut set_a = SocketSet::new(Vec::new());
  let mut set_b = SocketSet::new(Vec::new());

  let ha = set_a.add(timed_socket(TIMEOUT_MS as u64));
  let hb = set_b.add(timed_socket(TIMEOUT_MS as u64));
  set_b
    .get_mut::<tcp::Socket>(hb)
    .listen(7946)
    .expect("listen");
  let remote_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946);
  set_a
    .get_mut::<tcp::Socket>(ha)
    .connect(if_a.context(), to_endpoint(remote_b), 49_000u16)
    .expect("connect");

  // Complete the handshake.
  let mut t = 0i64;
  for _ in 0..50 {
    if_a.poll(SmolInstant::from_millis(t), &mut dev_a, &mut set_a);
    if_b.poll(SmolInstant::from_millis(t), &mut dev_b, &mut set_b);
    t += 1;
    if set_a.get::<tcp::Socket>(ha).may_send() && set_b.get::<tcp::Socket>(hb).may_send() {
      break;
    }
  }
  assert!(set_a.get::<tcp::Socket>(ha).may_send(), "A must establish");
  assert!(set_b.get::<tcp::Socket>(hb).may_send(), "B must establish");

  // Exchange data across 3x the timeout window, sending on each side every half
  // window so neither is ever idle for a full timeout. Both stay Established.
  let end = t + TIMEOUT_MS * 3;
  let mut last_send = t;
  while t < end {
    if t - last_send >= TIMEOUT_MS / 2 {
      // Ignoring Err: both sockets stay Established throughout and the 4 KiB rings
      // dwarf the few bytes sent, so send_slice always succeeds; the exchange
      // exists only to keep the inactivity timer fed.
      let _ = set_a.get_mut::<tcp::Socket>(ha).send_slice(b"ping");
      let _ = set_b.get_mut::<tcp::Socket>(hb).send_slice(b"pong");
      last_send = t;
    }
    if_a.poll(SmolInstant::from_millis(t), &mut dev_a, &mut set_a);
    if_b.poll(SmolInstant::from_millis(t), &mut dev_b, &mut set_b);
    t += 5;
  }

  assert!(
    set_a.get::<tcp::Socket>(ha).may_send(),
    "an actively-exchanging socket must not be reaped by the inactivity timeout"
  );
  assert!(
    set_b.get::<tcp::Socket>(hb).may_send(),
    "an actively-exchanging socket must not be reaped by the inactivity timeout"
  );
}
