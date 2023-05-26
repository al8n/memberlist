use std::{
  collections::{HashMap, VecDeque},
  net::SocketAddr,
  sync::{atomic::AtomicU32, Arc},
  time::Instant,
};

#[cfg(feature = "async")]
use async_lock::{Mutex, RwLock};
use bytes::Bytes;
use crossbeam_utils::CachePadded;
#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, RwLock};

#[cfg(feature = "async")]
use async_channel::{Receiver, Sender};
#[cfg(not(feature = "async"))]
use crossbeam_channel::{Receiver, Sender};

use showbiz_traits::{Delegate, Transport, VoidDelegate};
use showbiz_types::{Address, MessageType, Name, Node, NodeState};

use crate::{
  awareness::Awareness,
  broadcast::ShowbizBroadcast,
  dns::DNS,
  network::META_MAX_SIZE,
  queue::DefaultNodeCalculator,
  types::{Alive, Message},
  TransmitLimitedQueue,
};

use super::{
  error::Error, state::LocalNodeState, suspicion::Suspicion, types::PushNodeState, Options,
  SecretKeyring,
};

mod r#async;

impl Options {
  #[inline]
  pub fn into_builder<T: Transport>(self, t: T) -> ShowbizBuilder<T> {
    ShowbizBuilder::new(t).with_options(self)
  }
}

pub struct ShowbizBuilder<T, D = VoidDelegate> {
  opts: Options,
  transport: T,
  delegate: Option<D>,
  /// Holds all of the encryption keys used internally. It is
  /// automatically initialized using the SecretKey and SecretKeys values.
  keyring: Option<SecretKeyring>,
}

impl<T: Transport> ShowbizBuilder<T> {
  #[inline]
  pub fn new(transport: T) -> Self {
    Self {
      opts: Options::default(),
      transport,
      delegate: None,
      keyring: None,
    }
  }
}

impl<T, D> ShowbizBuilder<T, D>
where
  T: Transport,
  D: Delegate,
{
  #[inline]
  pub fn with_options(mut self, opts: Options) -> Self {
    self.opts = opts;
    self
  }

  #[inline]
  pub fn with_keyring(mut self, keyring: Option<SecretKeyring>) -> Self {
    self.keyring = keyring;
    self
  }

  #[inline]
  pub fn with_transport<NT>(self, t: NT) -> ShowbizBuilder<NT, D> {
    let Self {
      opts,
      delegate,
      keyring,
      ..
    } = self;

    ShowbizBuilder {
      opts,
      transport: t,
      delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_delegate<ND>(self, d: Option<ND>) -> ShowbizBuilder<T, ND> {
    let Self {
      opts,
      transport,
      delegate: _,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate: d,
      keyring,
    }
  }
}

#[viewit::viewit]
pub(crate) struct HotData {
  sequence_num: CachePadded<AtomicU32>,
  incarnation: CachePadded<AtomicU32>,
  push_pull_req: CachePadded<AtomicU32>,
  shutdown: CachePadded<AtomicU32>,
  leave: CachePadded<AtomicU32>,
  num_nodes: Arc<CachePadded<AtomicU32>>,
}

impl HotData {
  fn new() -> Self {
    Self {
      sequence_num: CachePadded::new(AtomicU32::new(0)),
      incarnation: CachePadded::new(AtomicU32::new(0)),
      num_nodes: Arc::new(CachePadded::new(AtomicU32::new(0))),
      push_pull_req: CachePadded::new(AtomicU32::new(0)),
      shutdown: CachePadded::new(AtomicU32::new(0)),
      leave: CachePadded::new(AtomicU32::new(0)),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Advertise {
  addr: SocketAddr,
}

#[viewit::viewit]
pub(crate) struct MessageHandoff {
  msg_ty: MessageType,
  buf: Bytes,
  from: SocketAddr,
}

#[viewit::viewit]
pub(crate) struct MessageQueue {
  /// high priority messages queue
  high: VecDeque<MessageHandoff>,
  /// low priority messages queue
  low: VecDeque<MessageHandoff>,
}

impl MessageQueue {
  const fn new() -> Self {
    Self {
      high: VecDeque::new(),
      low: VecDeque::new(),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Memberlist {
  /// self
  local: LocalNodeState,
  /// remote nodes
  nodes: Vec<LocalNodeState>,
  node_map: HashMap<Name, LocalNodeState>,
  node_timers: HashMap<Name, Suspicion>,
}

impl Memberlist {
  fn new(local: LocalNodeState) -> Self {
    Self {
      local,
      nodes: Vec::new(),
      node_map: HashMap::new(),
      node_timers: HashMap::new(),
    }
  }

  pub(crate) fn any_alive(&self) -> bool {
    self
      .nodes
      .iter()
      .any(|n| !n.dead_or_left() && n.node.name() != self.local.node.name())
  }
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<T: Transport, D = VoidDelegate> {
  hot: HotData,
  awareness: Awareness,
  advertise: RwLock<SocketAddr>,
  broadcast: TransmitLimitedQueue<ShowbizBroadcast, DefaultNodeCalculator>,
  shutdown_rx: Receiver<()>,
  shutdown_tx: Sender<()>,
  // Serializes calls to Leave
  leave_lock: Mutex<()>,
  leave_broadcast_tx: Sender<()>,
  leave_broadcast_rx: Receiver<()>,
  opts: Arc<Options>,
  transport: T,
  keyring: Option<SecretKeyring>,
  delegate: Option<D>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue>,
  nodes: RwLock<Memberlist>,
  dns: Option<DNS<T>>,
}

pub struct Showbiz<T: Transport, D = VoidDelegate> {
  pub(crate) inner: Arc<ShowbizCore<T, D>>,
}

impl<T, D> Clone for Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}
