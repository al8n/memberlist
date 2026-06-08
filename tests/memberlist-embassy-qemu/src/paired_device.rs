//! A `no_std` channel-backed paired [`embassy_net::driver::Driver`].
//!
//! This is the bare-metal twin of `memberlist-embassy`'s host-test
//! `support::paired_device`: two `PairedDevice`s are cross-wired by [`pair`] so
//! each device's transmit pushes a frame into the OTHER device's receive queue
//! and wakes the other stack, moving real ethernet frames between two embassy-net
//! stacks on a single-threaded executor. It is the in-memory link the two
//! memberlist nodes converge over, standing in for an MII/RMII peripheral.
//!
//! Everything runs on one thread-mode executor with interrupts the only
//! preemption, and the embassy-net stack only touches the device from task
//! context, so the shared queues are plain `Rc<RefCell<…>>` (no locking) exactly
//! as in the host port.

use alloc::{collections::VecDeque, rc::Rc, vec, vec::Vec};
use core::{
  cell::RefCell,
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
      mac: [0x02, 0, 0, 0, 0, 1],
    },
    PairedDevice {
      rx: a2b,
      tx: b2a,
      my_waker: waker_b,
      peer_waker: waker_a,
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
}

/// A receive token carrying one delivered frame.
pub struct PairedRx(Vec<u8>);

/// A transmit token that, on consume, writes the frame to the peer's receive
/// queue and wakes the peer's stack.
pub struct PairedTx {
  tx: Wire,
  peer_waker: WakerSlot,
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
    let mut buf = vec![0u8; len];
    let r = f(&mut buf);
    self.tx.borrow_mut().push_back(buf);
    // Wake the peer's stack so its `embassy_net::Runner` re-polls and drains this
    // frame; without this the peer would not process inbound traffic until some
    // unrelated event polled it.
    if let Some(w) = self.peer_waker.borrow_mut().take() {
      w.wake();
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
      },
    ))
  }

  fn transmit(&mut self, _cx: &mut Context<'_>) -> Option<PairedTx> {
    Some(PairedTx {
      tx: self.tx.clone(),
      peer_waker: self.peer_waker.clone(),
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
