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

use memberlist_proto::event::Event;

use crate::{
  command::Command,
  delegate::{AliveDelegate, Delegate, MergeDelegate},
  driver_options::DriverOptions,
  options::MemberlistOptions,
  snapshot::MemberlistSnapshot,
  transport::Transport,
};

// Helper type alias used in the `commands_rx` field: the command channel is
// parameterised over the transport id type so `Command::Ping` can carry a
// `Node<I, SocketAddr>` without an extra generic on `TransportRuntime`.
type CommandReceiver<T> = Receiver<Command<<T as Transport>::Id>>;

/// The driver-level CIDR source/peer filter threaded into the driver loop and
/// stored on the runtime: `Option<CidrPolicy>` with the `cidr` feature.
#[cfg(feature = "cidr")]
pub(crate) type CidrFilter = Option<memberlist_proto::CidrPolicy>;
/// Without the `cidr` feature the carrier is a distinct zero-sized type rather
/// than `()`, so the runtime field and the loop parameter exist unconditionally
/// (no cfg on the signatures) AND threading it as a driver-loop argument does not
/// trip `clippy::unit_arg` — which fires on a unit-typed argument expression
/// (e.g. a struct-field access) but not on this struct.
#[cfg(not(feature = "cidr"))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CidrFilter;

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
  pub(crate) commands_rx: CommandReceiver<T>,
  pub(crate) events_tx: Sender<Event<T::Id, SocketAddr>>,
  /// Drops at the `EventStream` fan-out (slow subscriber) — recoverable
  /// membership/control gaps.
  pub(crate) events_dropped: Arc<AtomicU64>,
  /// Drops at the observation (delegate) channel (delegate fell behind) — may
  /// include unrecoverable app-data.
  pub(crate) observation_dropped: Arc<AtomicU64>,
  pub(crate) snapshot: Arc<ArcSwap<MemberlistSnapshot<T::Id, SocketAddr>>>,
  /// The machine's load-shedding counters, republished by the driver on change.
  pub(crate) metrics: Arc<ArcSwap<memberlist_proto::metrics::Metrics>>,
  pub(crate) shutdown_flag: Arc<AtomicBool>,
  pub(crate) driver_options: DriverOptions,
  pub(crate) memberlist_options: MemberlistOptions,
  /// Optional machine alive-admission predicate; `T::run` installs it into
  /// the `Endpoint`. `None` admits all.
  pub(crate) alive_delegate: Option<Box<dyn AliveDelegate<T::Id, SocketAddr>>>,
  /// Optional machine merge-admission predicate; `T::run` installs it into
  /// the `Endpoint`. `None` accepts every join merge.
  pub(crate) merge_delegate: Option<Box<dyn MergeDelegate<T::Id, SocketAddr>>>,
  /// Driver-level CIDR source/peer filter (`()` when the `cidr` feature is off).
  pub(crate) cidr_policy: CidrFilter,
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
    commands_rx: CommandReceiver<T>,
    events_tx: Sender<Event<T::Id, SocketAddr>>,
    events_dropped: Arc<AtomicU64>,
    observation_dropped: Arc<AtomicU64>,
    snapshot: Arc<ArcSwap<MemberlistSnapshot<T::Id, SocketAddr>>>,
    metrics: Arc<ArcSwap<memberlist_proto::metrics::Metrics>>,
    shutdown_flag: Arc<AtomicBool>,
    driver_options: DriverOptions,
    memberlist_options: MemberlistOptions,
    alive_delegate: Option<Box<dyn AliveDelegate<T::Id, SocketAddr>>>,
    merge_delegate: Option<Box<dyn MergeDelegate<T::Id, SocketAddr>>>,
    cidr_policy: CidrFilter,
  ) -> Self {
    Self {
      delegate,
      commands_rx,
      events_tx,
      events_dropped,
      observation_dropped,
      snapshot,
      metrics,
      shutdown_flag,
      driver_options,
      memberlist_options,
      alive_delegate,
      merge_delegate,
      cidr_policy,
    }
  }
}
