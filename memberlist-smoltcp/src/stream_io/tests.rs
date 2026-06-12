use super::*;

use std::{cell::RefCell, collections::VecDeque, rc::Rc, vec, vec::Vec};

use smoltcp::{
  iface::{Config as IfConfig, Interface, SocketSet},
  phy::{ChecksumCapabilities, Device, DeviceCapabilities, Medium, RxToken, TxToken},
  time::Instant as SmolInstant,
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

  let local_a = SocketAddr::new(
    core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 1)),
    7946,
  );
  let remote_b = SocketAddr::new(
    core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 2)),
    7946,
  );

  // B listens; A dials. Both views borrow their own set for the call only.
  {
    let cell_b = RefCell::new(&mut set_b);
    let mut sb = SmoltcpStream::new(&mut if_b, &cell_b);
    sb.listen(hb, 7946).expect("listen");
  }
  {
    let cell_a = RefCell::new(&mut set_a);
    let mut sa = SmoltcpStream::new(&mut if_a, &cell_a);
    sa.connect(ha, remote_b, local_a.port()).expect("connect");
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
