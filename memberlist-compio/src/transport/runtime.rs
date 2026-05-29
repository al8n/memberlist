//! `TransportRuntime<T, D>` — the bundle handed to `T::run(self, runtime)`.
//!
//! Carries the delegate, command receiver, events sender, snapshot, and
//! driver / SWIM tuning knobs. The concrete machine endpoint is NOT carried
//! here: `Memberlist::new` is generic over `T` and cannot build the
//! backend's record-layer config + dial closures, so each `T::run` body
//! builds its own endpoint from the transport's stored config and then
//! delegates to the shared stream / QUIC driver loop, threading the
//! per-Transport-owned resources (gossip socket, TCP listener,
//! stream-specific knobs, etc.) alongside.

use std::{
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
  },
};

use arc_swap::ArcSwap;
use flume::{Receiver, Sender};

use memberlist_machine::event::Event;

use crate::{
  command::Command,
  delegate::{AliveDelegate, Delegate, MergeDelegate},
  driver_options::DriverOptions,
  options::MemberlistOptions,
  snapshot::MemberlistSnapshot,
  transport::Transport,
};

/// Bundle handed to `Transport::run(self, runtime)`. Carries the delegate,
/// command receiver, events sender, snapshot, and driver / SWIM tuning
/// knobs. The machine endpoint is built inside `T::run` (it needs the
/// backend's private record-layer config), so it is deliberately absent
/// from this bundle.
pub struct TransportRuntime<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = SocketAddr>,
{
  pub(crate) delegate: D,
  pub(crate) commands_rx: Receiver<Command>,
  pub(crate) events_tx: Sender<Event<T::Id, SocketAddr>>,
  /// Drops at the `EventStream` fan-out (slow subscriber) — recoverable
  /// membership/control gaps.
  pub(crate) events_dropped: Arc<AtomicU64>,
  /// Drops at the observation (delegate) channel (delegate fell behind) — may
  /// include unrecoverable app-data.
  pub(crate) observation_dropped: Arc<AtomicU64>,
  pub(crate) snapshot: Arc<ArcSwap<MemberlistSnapshot<T::Id, SocketAddr>>>,
  pub(crate) shutdown_flag: Arc<AtomicBool>,
  pub(crate) driver_options: DriverOptions,
  pub(crate) memberlist_options: MemberlistOptions,
  /// Optional machine alive-admission predicate; `T::run` installs it into
  /// the `Endpoint`. `None` admits all.
  pub(crate) alive_delegate: Option<Box<dyn AliveDelegate<T::Id, SocketAddr>>>,
  /// Optional machine merge-admission predicate; `T::run` installs it into
  /// the `Endpoint`. `None` accepts every join merge.
  pub(crate) merge_delegate: Option<Box<dyn MergeDelegate<T::Id, SocketAddr>>>,
}

impl<T, D> TransportRuntime<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = SocketAddr>,
{
  /// Constructor — called by `Memberlist::new`.
  #[allow(clippy::too_many_arguments)]
  #[inline]
  pub(crate) fn new(
    delegate: D,
    commands_rx: Receiver<Command>,
    events_tx: Sender<Event<T::Id, SocketAddr>>,
    events_dropped: Arc<AtomicU64>,
    observation_dropped: Arc<AtomicU64>,
    snapshot: Arc<ArcSwap<MemberlistSnapshot<T::Id, SocketAddr>>>,
    shutdown_flag: Arc<AtomicBool>,
    driver_options: DriverOptions,
    memberlist_options: MemberlistOptions,
    alive_delegate: Option<Box<dyn AliveDelegate<T::Id, SocketAddr>>>,
    merge_delegate: Option<Box<dyn MergeDelegate<T::Id, SocketAddr>>>,
  ) -> Self {
    Self {
      delegate,
      commands_rx,
      events_tx,
      events_dropped,
      observation_dropped,
      snapshot,
      shutdown_flag,
      driver_options,
      memberlist_options,
      alive_delegate,
      merge_delegate,
    }
  }
}
