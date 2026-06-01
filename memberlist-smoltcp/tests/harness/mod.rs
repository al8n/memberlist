//! Deterministic two-node test substrate: a paired in-memory smoltcp `Device`
//! (one node's TX is the other's RX) on `medium::Ip`, plus a virtual clock.
#![allow(dead_code)]

use std::{cell::RefCell, collections::VecDeque, rc::Rc};

use core::net::IpAddr;

use memberlist_smoltcp::{HardwareAddress, InterfaceConfig, IpCidr};
use smoltcp::{
  phy::{ChecksumCapabilities, Device, DeviceCapabilities, Medium, RxToken, TxToken},
  time::Instant,
};

/// Build an [`InterfaceConfig`] for the harness's `Medium::Ip` devices:
/// `HardwareAddress::Ip` plus the node's `/24` address. Keeps the construction
/// call sites tidy now that `Memberlist::new` takes a full interface config.
pub fn ip_iface(ip: IpAddr) -> InterfaceConfig {
  InterfaceConfig::new(HardwareAddress::Ip).with_ip_addr(IpCidr::new(ip.into(), 24))
}

type Wire = Rc<RefCell<VecDeque<Vec<u8>>>>;

/// One end of a virtual link: reads from `rx`, writes to `tx`.
pub struct PairedDevice {
  rx: Wire,
  tx: Wire,
  mtu: usize,
}

impl PairedDevice {
  /// Whether a frame has been delivered to this node's receive queue but not yet
  /// drained by a `poll`. A deadline-driven multi-node test loop uses this to
  /// model "an arriving packet wakes its receiver": while any node has an inbound
  /// frame pending, the loop re-polls promptly instead of sleeping the shared
  /// clock to a far timer — otherwise a reply sitting in a peer's rx between polls
  /// would not be processed until the next scheduled wake, inflating every
  /// zero-latency round trip to that interval.
  pub fn inbound_pending(&self) -> bool {
    !self.rx.borrow().is_empty()
  }
}

/// Build the two ends of one virtual link.
///
/// Frames sent by the `A` end arrive at the `B` end's receive queue, and vice
/// versa — the two FIFOs are cross-wired so `A.tx == B.rx` and `B.tx == A.rx`.
pub fn link(mtu: usize) -> (PairedDevice, PairedDevice) {
  let a2b: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let b2a: Wire = Rc::new(RefCell::new(VecDeque::new()));
  (
    PairedDevice {
      rx: b2a.clone(),
      tx: a2b.clone(),
      mtu,
    },
    PairedDevice {
      rx: a2b,
      tx: b2a,
      mtu,
    },
  )
}

/// One central node wired to two peers over a shared IP segment.
///
/// `receive` drains inbound frames from EITHER peer (peer-1 queue first, then
/// peer-2), and `transmit` broadcasts each outbound frame onto BOTH peer receive
/// queues. On `Medium::Ip` each peer's smoltcp interface drops any packet not
/// addressed to its own IP, so a broadcast TX is delivered only to the intended
/// peer in practice — exactly a shared link with two distinct unicast peers.
///
/// This lets a single `Memberlist::poll(&mut hub_device)` reach two independent
/// reachable peers at once, which a strictly point-to-point [`PairedDevice`]
/// cannot. Used to exercise two concurrent outbound reliable exchanges from one
/// node (e.g. a `join` with two seeds) where the second must defer to a
/// `PendingDial` behind the first.
pub struct HubDevice {
  /// Inbound frames from peer 1 (peer-1 TX → hub RX).
  rx1: Wire,
  /// Inbound frames from peer 2 (peer-2 TX → hub RX).
  rx2: Wire,
  /// Outbound frames to peer 1 (hub TX broadcast → peer-1 RX).
  tx1: Wire,
  /// Outbound frames to peer 2 (hub TX broadcast → peer-2 RX).
  tx2: Wire,
  mtu: usize,
}

impl HubDevice {
  /// Whether either peer has delivered a frame to the hub's receive queues that a
  /// `poll` has not yet drained. See [`PairedDevice::inbound_pending`].
  pub fn inbound_pending(&self) -> bool {
    !self.rx1.borrow().is_empty() || !self.rx2.borrow().is_empty()
  }
}

/// Build a three-node hub: the central node's [`HubDevice`] plus the two peers'
/// [`PairedDevice`]s.
///
/// Frames the hub sends are broadcast to both peers; frames each peer sends reach
/// the hub. The two peers are NOT wired to each other — they can communicate only
/// indirectly through the hub — so with periodic schedulers disabled the hub
/// learns each peer ONLY via its own direct reliable exchange to that peer.
pub fn hub(mtu: usize) -> (HubDevice, PairedDevice, PairedDevice) {
  let h2p1: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let p1h: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let h2p2: Wire = Rc::new(RefCell::new(VecDeque::new()));
  let p2h: Wire = Rc::new(RefCell::new(VecDeque::new()));
  (
    HubDevice {
      rx1: p1h.clone(),
      rx2: p2h.clone(),
      tx1: h2p1.clone(),
      tx2: h2p2.clone(),
      mtu,
    },
    PairedDevice {
      rx: h2p1,
      tx: p1h,
      mtu,
    },
    PairedDevice {
      rx: h2p2,
      tx: p2h,
      mtu,
    },
  )
}

pub struct VRx(Vec<u8>);
pub struct VTx(Wire);

/// A TX token that writes the same outbound frame to two peer receive queues.
pub struct VTxBroadcast(Wire, Wire);

impl TxToken for VTxBroadcast {
  fn consume<R, F>(self, len: usize, f: F) -> R
  where
    F: FnOnce(&mut [u8]) -> R,
  {
    let mut buf = vec![0u8; len];
    let r = f(&mut buf);
    self.0.borrow_mut().push_back(buf.clone());
    self.1.borrow_mut().push_back(buf);
    r
  }
}

impl Device for HubDevice {
  type RxToken<'a>
    = VRx
  where
    Self: 'a;
  type TxToken<'a>
    = VTxBroadcast
  where
    Self: 'a;

  fn receive(&mut self, _timestamp: Instant) -> Option<(VRx, VTxBroadcast)> {
    // Drain peer 1 first, then peer 2; whichever has a frame is delivered.
    let frame = self
      .rx1
      .borrow_mut()
      .pop_front()
      .or_else(|| self.rx2.borrow_mut().pop_front())?;
    Some((VRx(frame), VTxBroadcast(self.tx1.clone(), self.tx2.clone())))
  }

  fn transmit(&mut self, _timestamp: Instant) -> Option<VTxBroadcast> {
    Some(VTxBroadcast(self.tx1.clone(), self.tx2.clone()))
  }

  fn capabilities(&self) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.medium = Medium::Ip;
    caps.max_transmission_unit = self.mtu;
    caps.checksum = ChecksumCapabilities::ignored();
    caps
  }
}

impl RxToken for VRx {
  fn consume<R, F>(self, f: F) -> R
  where
    F: FnOnce(&[u8]) -> R,
  {
    f(&self.0)
  }
}

impl TxToken for VTx {
  fn consume<R, F>(self, len: usize, f: F) -> R
  where
    F: FnOnce(&mut [u8]) -> R,
  {
    let mut buf = vec![0u8; len];
    let r = f(&mut buf);
    self.0.borrow_mut().push_back(buf);
    r
  }
}

impl Device for PairedDevice {
  type RxToken<'a>
    = VRx
  where
    Self: 'a;
  type TxToken<'a>
    = VTx
  where
    Self: 'a;

  fn receive(&mut self, _timestamp: Instant) -> Option<(VRx, VTx)> {
    let frame = self.rx.borrow_mut().pop_front()?;
    Some((VRx(frame), VTx(self.tx.clone())))
  }

  fn transmit(&mut self, _timestamp: Instant) -> Option<VTx> {
    Some(VTx(self.tx.clone()))
  }

  fn capabilities(&self) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.medium = Medium::Ip;
    caps.max_transmission_unit = self.mtu;
    caps.checksum = ChecksumCapabilities::ignored();
    caps
  }
}

/// Deterministic virtual clock anchored away from zero.
///
/// `memberlist_proto::Instant - Duration` saturates at the origin; the
/// 86 400 s offset gives backward-aging headroom for any suspicion or failure
/// timers that need to subtract from `now`.
pub struct Clock {
  ms: u64,
}

impl Clock {
  pub fn new() -> Self {
    Self { ms: 86_400_000 }
  }

  pub fn now(&self) -> memberlist_proto::Instant {
    memberlist_proto::Instant::from_origin(core::time::Duration::from_millis(self.ms))
  }

  pub fn advance_ms(&mut self, by: u64) {
    self.ms += by;
  }

  /// Jump the clock forward to `target` (a deadline returned by `poll`),
  /// rounding UP to the next whole millisecond. Used by a deadline-driven
  /// super-loop that sleeps precisely to the returned wakeup rather than ticking
  /// a fixed step, so a test exercises the real wake contract.
  ///
  /// This clock has millisecond resolution while a `poll` deadline can carry
  /// microseconds (smoltcp instants are µs-precise). A monotonic clock cannot
  /// wake BEFORE a deadline, so a sub-millisecond target must round up to the
  /// next tick at or after it — otherwise truncation would land the clock just
  /// shy of a future deadline and a `now`-equal re-poll would spin in place.
  /// Never moves backwards: a `target` already at or before the current reading
  /// leaves the clock put, matching a clock that cannot rewind.
  pub fn advance_to(&mut self, target: memberlist_proto::Instant) {
    let ns = target.since_origin().as_nanos();
    // Ceiling division to whole milliseconds.
    let target_ms = ns.div_ceil(1_000_000) as u64;
    if target_ms > self.ms {
      self.ms = target_ms;
    }
  }
}
