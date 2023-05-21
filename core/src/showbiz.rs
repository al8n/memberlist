use std::{
  collections::{HashMap, VecDeque},
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, AtomicU32},
    Arc,
  },
};

#[cfg(feature = "async")]
use async_lock::{Mutex, RwLock};
use bytes::{BufMut, Bytes};
use crossbeam_utils::CachePadded;
use futures_util::io::BufReader;
#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, RwLock};

#[cfg(feature = "async")]
use async_channel::{Receiver, Sender};
#[cfg(not(feature = "async"))]
use crossbeam_channel::{Receiver, Sender};

use showbiz_traits::{Connection, Delegate, Transport, VoidDelegate};
use showbiz_types::{Address, MessageType, Name, Node};

use crate::{label::LabeledConnection, types::Message};

use super::{
  error::Error, state::LocalNodeState, suspicion::Suspicion, types::PushNodeState, Options,
  SecretKeyring,
};

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

  pub fn finalize(self) -> Showbiz<T, D> {
    let Self {
      opts,
      transport,
      delegate,
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
        delegate: delegate.map(Arc::new),
        keyring,
        shutdown_rx,
        shutdown_tx,
        handoff_tx,
        handoff_rx,
        queue: Mutex::new(MessageQueue::new()),
        nodes: RwLock::new(Default::default()),
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
  shutdown: CachePadded<AtomicU32>,
  leave: CachePadded<AtomicU32>,
}

impl HotData {
  const fn new() -> Self {
    Self {
      sequence_num: CachePadded::new(AtomicU32::new(0)),
      incarnation: CachePadded::new(AtomicU32::new(0)),
      num_nodes: CachePadded::new(AtomicU32::new(0)),
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
pub(crate) struct Nodes {
  nodes: Vec<LocalNodeState>,
  node_map: HashMap<SocketAddr, LocalNodeState>,
  node_timers: HashMap<SocketAddr, Suspicion>,
}

impl Default for Nodes {
  fn default() -> Self {
    Self {
      nodes: Vec::new(),
      node_map: HashMap::new(),
      node_timers: HashMap::new(),
    }
  }
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<T: Transport, D = VoidDelegate> {
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
  delegate: Option<Arc<D>>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue>,

  nodes: RwLock<Nodes>,
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

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  /// Uses the unreliable packet-oriented interface of the transport
  /// to target a user message at the given node (this does not use the gossip
  /// mechanism). The maximum size of the message depends on the configured
  /// `packet_buffer_size` for this memberlist instance.
  pub async fn send_best_effort(&self, to: &Node, mut msg: Vec<u8>) -> Result<(), Error<T, D>> {
    // Encode as a user message

    // TODO: implement
    Ok(())
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  #[inline]
  pub async fn send_reliable(&self, to: &Node, mut msg: Message) -> Result<(), Error<T, D>> {
    self.send_user_msg(to.full_address(), msg).await
  }

  async fn send_user_msg(&self, addr: &Address, msg: Message) -> Result<(), Error<T, D>> {
    if addr.name().is_empty() && self.inner.opts.require_node_names {
      return Err(Error::MissingNodeName);
    }
    let mut conn = self
      .inner
      .transport
      .dial_timeout(addr.addr(), self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;

    // self
    //   .raw_send_msg_stream(
    //     LabeledConnection::new(BufReader::new(conn)),
    //     msg.freeze(),
    //     Some(addr.addr()).as_ref(),
    //     self.encryption_enabled().await,
    //   )
    //   .await
    todo!()
  }

  /// Returns a list of all known live nodes.
  #[inline]
  pub async fn members(&self) -> Vec<Arc<Node>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .map(|n| n.node.clone())
      .collect()
  }

  #[inline]
  pub(crate) fn has_shutdown(&self) -> bool {
    self
      .inner
      .hot
      .shutdown
      .load(std::sync::atomic::Ordering::SeqCst)
      == 1
  }

  #[inline]
  pub(crate) fn has_left(&self) -> bool {
    self
      .inner
      .hot
      .leave
      .load(std::sync::atomic::Ordering::SeqCst)
      == 1
  }

  #[cfg(test)]
  pub(crate) async fn change_node<F>(&self, addr: SocketAddr, mut f: F)
  where
    F: Fn(&LocalNodeState),
  {
    let mut nodes = self.inner.nodes.write().await;
    if let Some(n) = nodes.node_map.get_mut(&addr) {
      f(n)
    }
  }

  pub(crate) async fn verify_protocol(&self, remote: &[PushNodeState]) -> Result<(), Error<T, D>> {
    // TODO: implement

    Ok(())
  }

  pub(crate) async fn encryption_enabled(&self) -> bool {
    if let Some(keyring) = &self.inner.keyring {
      !keyring.lock().await.is_empty()
    } else {
      false
    }
  }
}
