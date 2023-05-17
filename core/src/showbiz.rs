use std::{
  collections::VecDeque,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, AtomicU32},
    Arc,
  },
};

#[cfg(feature = "async")]
use async_lock::{Mutex, RwLock};
use crossbeam_utils::CachePadded;
#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, RwLock};

#[cfg(feature = "async")]
use async_channel::{Receiver, Sender};
#[cfg(not(feature = "async"))]
use crossbeam_channel::{Receiver, Sender};

use showbiz_traits::{
  AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, PingDelegate, Transport,
  VoidAliveDelegate, VoidConflictDelegate, VoidDelegate, VoidEventDelegate, VoidMergeDelegate,
  VoidPingDelegate,
};

use crate::{error::Error, Options, SecretKeyring};

impl Options {
  #[inline]
  pub fn into_builder<T: Transport>(self, t: T) -> ShowbizBuilder<T> {
    ShowbizBuilder::new(t).with_options(self)
  }
}

pub struct ShowbizBuilder<
  T,
  D = VoidDelegate,
  ED = VoidEventDelegate,
  CD = VoidConflictDelegate,
  MD = VoidMergeDelegate<Error>,
  PD = VoidPingDelegate,
  AD = VoidAliveDelegate<Error>,
> {
  opts: Options,
  transport: T,
  delegate: Option<D>,
  event_delegate: Option<ED>,
  conflict_delegate: Option<CD>,
  merge_delegate: Option<MD>,
  ping_delegate: Option<PD>,
  alive_delegate: Option<AD>,
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
      event_delegate: None,
      conflict_delegate: None,
      merge_delegate: None,
      ping_delegate: None,
      alive_delegate: None,
      keyring: None,
    }
  }
}

impl<T, D, ED, CD, MD, PD, AD> ShowbizBuilder<T, D, ED, CD, MD, PD, AD>
where
  T: Transport,
  D: Delegate,
  ED: EventDelegate,
  CD: ConflictDelegate,
  MD: MergeDelegate,
  PD: PingDelegate,
  AD: AliveDelegate,
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
  pub fn with_transport<NT>(self, t: NT) -> ShowbizBuilder<NT, D, ED, CD, MD, PD, AD> {
    let Self {
      opts,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
      ..
    } = self;

    ShowbizBuilder {
      opts,
      transport: t,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_delegate<ND>(self, d: Option<ND>) -> ShowbizBuilder<T, ND, ED, CD, MD, PD, AD> {
    let Self {
      event_delegate,
      opts,
      transport,
      delegate: _,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate: d,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_event_delegate<NED>(
    self,
    ed: Option<NED>,
  ) -> ShowbizBuilder<T, D, NED, CD, MD, PD, AD> {
    let Self {
      event_delegate: _,
      opts,
      transport,
      delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate,
      event_delegate: ed,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_conflict_delegate<NCD>(
    self,
    cd: Option<NCD>,
  ) -> ShowbizBuilder<T, D, ED, NCD, MD, PD, AD> {
    let Self {
      conflict_delegate: _,
      opts,
      transport,
      delegate,
      event_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate: cd,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_merge_delegate<NMD>(
    self,
    md: Option<NMD>,
  ) -> ShowbizBuilder<T, D, ED, CD, NMD, PD, AD> {
    let Self {
      merge_delegate: _,
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate: md,
      ping_delegate,
      alive_delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_ping_delegate<NPD>(
    self,
    pd: Option<NPD>,
  ) -> ShowbizBuilder<T, D, ED, CD, MD, NPD, AD> {
    let Self {
      ping_delegate: _,
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      alive_delegate,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate: pd,
      alive_delegate,
      keyring,
    }
  }

  #[inline]
  pub fn with_alive_delegate<NAD>(
    self,
    ad: Option<NAD>,
  ) -> ShowbizBuilder<T, D, ED, CD, MD, PD, NAD> {
    let Self {
      alive_delegate: _,
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      keyring,
    } = self;

    ShowbizBuilder {
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate: ad,
      keyring,
    }
  }

  pub fn finalize(self) -> Showbiz<T, D, ED, CD, MD, PD, AD> {
    let Self {
      opts,
      transport,
      delegate,
      event_delegate,
      conflict_delegate,
      merge_delegate,
      ping_delegate,
      alive_delegate,
      keyring,
    } = self;

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);

    Showbiz {
      inner: Arc::new(ShowbizCore {
        hot: HotData::new(),
        advertise: todo!(),
        shutdown_lock: Mutex::new(()),
        leave_lock: Mutex::new(()),
        opts: Arc::new(opts),
        transport: Arc::new(transport),
        delegates: Arc::new(ShowbizDelegates {
          delegate: delegate.map(Arc::new),
          event_delegate: event_delegate.map(Arc::new),
          conflict_delegate: conflict_delegate.map(Arc::new),
          merge_delegate: merge_delegate.map(Arc::new),
          ping_delegate: ping_delegate.map(Arc::new),
          alive_delegate: alive_delegate.map(Arc::new),
        }),
        keyring,
        shutdown_rx,
        shutdown_tx,
        handoff_tx,
        handoff_rx,
        queue: Mutex::new(MessageQueue::new()),
      }),
    }
  }
}

#[viewit::viewit]
pub(crate) struct HotData {
  sequence_num: CachePadded<AtomicU32>,
  incarnation: CachePadded<AtomicU32>,
  num_nodes: CachePadded<AtomicU32>,
  push_pull_req: CachePadded<AtomicU32>,
  shutdown: CachePadded<AtomicBool>,
  leave: CachePadded<AtomicBool>,
}

impl HotData {
  const fn new() -> Self {
    Self {
      sequence_num: CachePadded::new(AtomicU32::new(0)),
      incarnation: CachePadded::new(AtomicU32::new(0)),
      num_nodes: CachePadded::new(AtomicU32::new(0)),
      push_pull_req: CachePadded::new(AtomicU32::new(0)),
      shutdown: CachePadded::new(AtomicBool::new(false)),
      leave: CachePadded::new(AtomicBool::new(false)),
    }
  }
}

#[viewit::viewit]
pub(crate) struct Advertise {
  addr: SocketAddr,
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizDelegates<
  D = VoidDelegate,
  ED = VoidEventDelegate,
  CD = VoidConflictDelegate,
  MD = VoidMergeDelegate<Error>,
  PD = VoidPingDelegate,
  AD = VoidAliveDelegate<Error>,
> {
  delegate: Option<Arc<D>>,
  event_delegate: Option<Arc<ED>>,
  conflict_delegate: Option<Arc<CD>>,
  merge_delegate: Option<Arc<MD>>,
  ping_delegate: Option<Arc<PD>>,
  alive_delegate: Option<Arc<AD>>,
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

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<
  T: Transport,
  D = VoidDelegate,
  ED = VoidEventDelegate,
  CD = VoidConflictDelegate,
  MD = VoidMergeDelegate<Error>,
  PD = VoidPingDelegate,
  AD = VoidAliveDelegate<Error>,
> {
  hot: HotData,
  advertise: RwLock<SocketAddr>,
  // Serializes calls to Shutdown
  shutdown_lock: Mutex<()>,
  shutdown_rx: Receiver<()>,
  shutdown_tx: Sender<()>,
  // Serializes calls to Leave
  leave_lock: Mutex<()>,
  opts: Arc<Options>,
  transport: Arc<T>,
  keyring: Option<SecretKeyring>,
  delegates: Arc<ShowbizDelegates<D, ED, CD, MD, PD, AD>>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue>,
}

pub struct Showbiz<
  T: Transport,
  D = VoidDelegate,
  ED = VoidEventDelegate,
  CD = VoidConflictDelegate,
  MD = VoidMergeDelegate<Error>,
  PD = VoidPingDelegate,
  AD = VoidAliveDelegate<Error>,
> {
  pub(crate) inner: Arc<ShowbizCore<T, D, ED, CD, MD, PD, AD>>,
}

impl<T, D, ED, CD, MD, PD, AD> Clone for Showbiz<T, D, ED, CD, MD, PD, AD>
where
  T: Transport,
  D: Delegate,
  ED: EventDelegate,
  CD: ConflictDelegate,
  MD: MergeDelegate,
  PD: PingDelegate,
  AD: AliveDelegate,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}
