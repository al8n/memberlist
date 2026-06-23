//! Snapshot of memberlist state, published into a single-owner cell.

use core::net::SocketAddr;
use std::{cell::RefCell, rc::Rc};

pub use memberlist_driver::MemberlistSnapshot;

/// The published-snapshot cell shared by the handle and the driver. Single-owner
/// per thread (thread-per-core): the driver swaps in a fresh
/// `Rc<MemberlistSnapshot>` after each state-affecting tick, and the handle (and
/// the driver) read the current one. The `RefCell` is a borrow check, not a lock
/// — handle and driver share one thread and never borrow it at overlapping times.
pub(crate) type SnapshotCell<I, A = SocketAddr> = Rc<RefCell<Rc<MemberlistSnapshot<I, A>>>>;
