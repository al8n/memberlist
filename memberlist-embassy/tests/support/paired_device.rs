//! A channel-backed paired [`embassy_net_driver::Driver`] for host loopback tests.
//!
//! Two `PairedDevice`s are cross-wired by [`pair`]: each device's transmit pushes
//! a frame into a shared channel that is the OTHER device's receive channel, and
//! wakes the other device's stack so its `embassy_net::Runner` re-polls and drains
//! the frame. This moves real ethernet frames between two embassy-net stacks on a
//! single `block_on` thread — the substrate the loopback tests run a two-node
//! memberlist cluster over.
//!
//! Modeled on `hick-embassy`'s `NullDriver`, but byte-moving: where `NullDriver`
//! discards transmits and never receives, this delivers each transmitted frame to
//! the paired device's receive queue and signals readiness via a shared `Waker`.

#![allow(dead_code)]

use std::{
  cell::{Cell, RefCell},
  collections::VecDeque,
  rc::Rc,
  task::{Context, Waker},
};

use embassy_net::driver::{Capabilities, Driver, HardwareAddress, LinkState, RxToken, TxToken};

/// The link MTU advertised by the paired devices (a standard ethernet frame).
const MTU: usize = 1514;

/// A shared frame FIFO between the two devices.
type Wire = Rc<RefCell<VecDeque<Vec<u8>>>>;
/// A shared slot holding a stack's most recent receive waker.
type WakerSlot = Rc<RefCell<Option<Waker>>>;

/// One end of a cross-wired virtual ethernet link.
///
/// Reads frames from `rx` (frames the peer transmitted) and writes frames to `tx`
/// (delivered to the peer's `rx`). `peer_waker` is the peer stack's receive waker,
/// woken on every transmit so the peer promptly drains the frame.
pub struct PairedDevice {
  rx: Wire,
  tx: Wire,
  /// This device's own receive waker, registered by `receive`/`link_state` and
  /// woken by the peer's transmit.
  my_waker: WakerSlot,
  /// The peer device's receive waker, woken by this device's transmit.
  peer_waker: WakerSlot,
  /// When `false`, this device's transmits are DROPPED (not delivered to the peer,
  /// no peer wake) — simulating a peer/link that has gone silent (stops ACKing). A
  /// test closes this via [`PairedDevice::tx_gate`] to exercise a worker's socket
  /// inactivity timeout.
  tx_open: Rc<Cell<bool>>,
  mac: [u8; 6],
}

/// Build the two ends of one virtual link, cross-wiring their channels and wakers.
///
/// Frames device `A` transmits arrive at device `B`'s receive queue (and wake
/// `B`'s stack), and vice versa.
pub fn pair() -> (PairedDevice, PairedDevice) {
  let a2b: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let b2a: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let waker_a: WakerSlot = Rc::new(RefCell::new(None));
  let waker_b: WakerSlot = Rc::new(RefCell::new(None));
  (
    PairedDevice {
      rx: b2a.clone(),
      tx: a2b.clone(),
      my_waker: waker_a.clone(),
      peer_waker: waker_b.clone(),
      tx_open: Rc::new(Cell::new(true)),
      mac: [0x02, 0, 0, 0, 0, 1],
    },
    PairedDevice {
      rx: a2b,
      tx: b2a,
      my_waker: waker_b,
      peer_waker: waker_a,
      tx_open: Rc::new(Cell::new(true)),
      mac: [0x02, 0, 0, 0, 0, 2],
    },
  )
}

impl PairedDevice {
  /// Register the current task's waker as this device's receive waker, so the
  /// peer's transmit can wake the stack.
  fn register(&self, cx: &mut Context<'_>) {
    *self.my_waker.borrow_mut() = Some(cx.waker().clone());
  }

  /// A handle to this device's transmit gate. Setting it to `false` drops every
  /// subsequent frame this device transmits, so the peer stops receiving (and so
  /// stops ACKing) — letting a test exercise a worker's socket inactivity timeout.
  pub fn tx_gate(&self) -> Rc<Cell<bool>> {
    self.tx_open.clone()
  }
}

/// A receive token carrying one delivered frame.
pub struct PairedRx(Vec<u8>);

/// A transmit token that, on consume, writes the frame to the peer's receive
/// queue and wakes the peer's stack.
pub struct PairedTx {
  tx: Wire,
  peer_waker: WakerSlot,
  tx_open: Rc<Cell<bool>>,
}

impl RxToken for PairedRx {
  fn consume<R, F>(self, f: F) -> R
  where
    F: FnOnce(&mut [u8]) -> R,
  {
    let mut buf = self.0;
    f(&mut buf)
  }
}

impl TxToken for PairedTx {
  fn consume<R, F>(self, len: usize, f: F) -> R
  where
    F: FnOnce(&mut [u8]) -> R,
  {
    let mut buf = std::vec![0u8; len];
    let r = f(&mut buf);
    // A closed tx gate DROPS the frame, simulating a peer/link gone silent: it never
    // reaches the peer and never wakes it, so the peer stops ACKing.
    if self.tx_open.get() {
      self.tx.borrow_mut().push_back(buf);
      // Wake the peer's stack so its `embassy_net::Runner` re-polls and drains this
      // frame; without this the peer would not process inbound traffic until some
      // unrelated event polled it.
      if let Some(w) = self.peer_waker.borrow_mut().take() {
        w.wake();
      }
    }
    r
  }
}

impl Driver for PairedDevice {
  type RxToken<'a>
    = PairedRx
  where
    Self: 'a;
  type TxToken<'a>
    = PairedTx
  where
    Self: 'a;

  fn receive(&mut self, cx: &mut Context<'_>) -> Option<(PairedRx, PairedTx)> {
    // Always (re)register the waker so a frame arriving after this poll wakes us.
    self.register(cx);
    let frame = self.rx.borrow_mut().pop_front()?;
    Some((
      PairedRx(frame),
      PairedTx {
        tx: self.tx.clone(),
        peer_waker: self.peer_waker.clone(),
        tx_open: self.tx_open.clone(),
      },
    ))
  }

  fn transmit(&mut self, _cx: &mut Context<'_>) -> Option<PairedTx> {
    Some(PairedTx {
      tx: self.tx.clone(),
      peer_waker: self.peer_waker.clone(),
      tx_open: self.tx_open.clone(),
    })
  }

  fn link_state(&mut self, cx: &mut Context<'_>) -> LinkState {
    // Keep the waker fresh on the link-state poll path too (the stack polls this),
    // so a frame delivered between data polls still wakes the stack.
    self.register(cx);
    LinkState::Up
  }

  fn capabilities(&self) -> Capabilities {
    let mut caps = Capabilities::default();
    caps.max_transmission_unit = MTU;
    caps
  }

  fn hardware_address(&self) -> HardwareAddress {
    HardwareAddress::Ethernet(self.mac)
  }
}
