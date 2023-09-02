use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

use crate::Name;

use super::*;

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum MockDelegateError {
  #[error("custom merge cancelled")]
  CustomMergeCancelled,
  #[error("custom alive cancelled")]
  CustomAliveCancelled,
}

#[derive(Debug, Eq, PartialEq)]
pub enum MockDelegateType {
  None,
  CancelMerge,
  CancelAlive,
  NotifyConflict,
  Ping,
  UserData,
}

#[derive(Default)]
struct MockDelegateInner {
  meta: Bytes,
  msgs: Vec<Bytes>,
  broadcasts: Vec<Message>,
  state: Bytes,
  remote_state: Bytes,
  conflict_existing: Option<Arc<Node>>,
  conflict_other: Option<Arc<Node>>,
  ping_other: Option<Arc<Node>>,
  ping_rtt: Duration,
  ping_payload: Bytes,
}

pub struct MockDelegate {
  inner: Arc<Mutex<MockDelegateInner>>,
  invoked: Arc<AtomicBool>,
  ty: MockDelegateType,
  ignore: Name,
  count: Arc<AtomicUsize>,
}

impl Default for MockDelegate {
  fn default() -> Self {
    Self::new()
  }
}

impl MockDelegate {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::None,
      ignore: Name::from_static_unchecked("mock"),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn cancel_merge() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::CancelMerge,
      ignore: Name::from_static_unchecked("mock"),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn is_invoked(&self) -> bool {
    self.invoked.load(atomic::Ordering::SeqCst)
  }

  pub fn cancel_alive(ignore: Name) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::CancelAlive,
      ignore,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn notify_conflict() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::NotifyConflict,
      ignore: Name::from_static_unchecked("mock"),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn ping() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::NotifyConflict,
      ignore: Name::from_static_unchecked("mock"),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn user_data() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::UserData,
      ignore: Name::from_static_unchecked("mock"),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn count(&self) -> usize {
    self.count.load(atomic::Ordering::SeqCst)
  }
}

impl MockDelegate {
  pub fn set_meta(&self, meta: Bytes) {
    self.inner.lock().meta = meta;
  }

  pub fn set_state(&self, state: Bytes) {
    self.inner.lock().state = state;
  }

  pub fn set_broadcasts(&self, broadcasts: Vec<Message>) {
    self.inner.lock().broadcasts = broadcasts;
  }

  pub fn get_remote_state(&self) -> Bytes {
    self.inner.lock().remote_state.clone()
  }

  pub fn get_messages(&self) -> Vec<Bytes> {
    let mut mu = self.inner.lock();
    let mut out = vec![];
    core::mem::swap(&mut out, &mut mu.msgs);
    out
  }

  pub fn get_contents(&self) -> Option<(Arc<Node>, Duration, Bytes)> {
    if self.ty == MockDelegateType::Ping {
      let mut mu = self.inner.lock();
      let other = mu.ping_other.take()?;
      let rtt = mu.ping_rtt;
      let payload = mu.ping_payload.clone();
      Some((other, rtt, payload))
    } else {
      None
    }
  }
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl Delegate for MockDelegate {
  type Error = MockDelegateError;

  fn node_meta(&self, _limit: usize) -> Bytes {
    self.inner.lock().meta.clone()
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error> {
    self.inner.lock().msgs.push(msg);
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn get_broadcasts(
    &self,
    _overhead: usize,
    _limit: usize,
  ) -> Result<Vec<Message>, Self::Error> {
    let mut mu = self.inner.lock();
    let mut out = vec![];
    core::mem::swap(&mut out, &mut mu.broadcasts);
    Ok(out)
  }

  #[cfg(not(feature = "nightly"))]
  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(self.inner.lock().state.clone())
  }

  #[cfg(not(feature = "nightly"))]
  async fn merge_remote_state(&self, buf: &[u8], _join: bool) -> Result<(), Self::Error> {
    self.inner.lock().remote_state = buf.to_owned().into();
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_join(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_leave(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_update(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_alive(&self, peer: Arc<Node>) -> Result<(), Self::Error> {
    match self.ty {
      MockDelegateType::CancelAlive => {
        self.count.fetch_add(1, atomic::Ordering::SeqCst);
        if peer.id().name() == &self.ignore {
          return Ok(());
        }
        tracing::info!(target = "showbiz.mock.delegate", "cancel alive");
        Err(MockDelegateError::CustomAliveCancelled)
      }
      _ => Ok(()),
    }
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_conflict(
    &self,
    existing: Arc<Node>,
    other: Arc<Node>,
  ) -> Result<(), Self::Error> {
    if self.ty == MockDelegateType::NotifyConflict {
      let mut inner = self.inner.lock();
      inner.conflict_existing = Some(existing);
      inner.conflict_other = Some(other);
    }

    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, _peers: Vec<Arc<Node>>) -> Result<(), Self::Error> {
    match self.ty {
      MockDelegateType::CancelMerge => {
        use atomic::Ordering;
        tracing::info!(target = "showbiz.mock.delegate", "cancel merge");
        self.invoked.store(true, Ordering::SeqCst);
        Err(MockDelegateError::CustomMergeCancelled)
      }
      _ => Ok(()),
    }
  }

  #[cfg(not(feature = "nightly"))]
  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    if self.ty == MockDelegateType::Ping {
      return Ok(Bytes::from_static(b"whatever"));
    }
    Ok(Bytes::new())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_ping_complete(
    &self,
    node: Arc<Node>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error> {
    if self.ty == MockDelegateType::Ping {
      let mut inner = self.inner.lock();
      inner.ping_other = Some(node);
      inner.ping_rtt = rtt;
      inner.ping_payload = payload;
    }
    Ok(())
  }

  #[cfg(feature = "nightly")]
  fn notify_user_msg<'a>(
    &'a self,
    _msg: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn get_broadcasts<'a>(
    &'a self,
    _overhead: usize,
    _limit: usize,
  ) -> impl Future<Output = Result<Vec<Message>, Self::Error>> + Send + 'a {
    async move { Ok(Vec::new()) }
  }

  #[cfg(feature = "nightly")]
  fn local_state<'a>(
    &'a self,
    _join: bool,
  ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'a {
    async move { Ok(Bytes::new()) }
  }

  #[cfg(feature = "nightly")]
  fn merge_remote_state<'a>(
    &'a self,
    _buf: Bytes,
    _join: bool,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_join<'a>(
    &'a self,
    _node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_leave<'a>(
    &'a self,
    _node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_update<'a>(
    &'a self,
    _node: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_alive<'a>(
    &'a self,
    _peer: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_conflict<'a>(
    &'a self,
    _existing: Arc<Node>,
    _other: Arc<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[cfg(feature = "nightly")]
  fn notify_merge<'a>(
    &'a self,
    _peers: Vec<Arc<Node>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move {
      match self.ty {
        MockDelegateType::None => Ok(()),
        MockDelegateType::CancelMerge => {
          use atomic::Ordering;
          tracing::info!(target = "showbiz.mock.delegate", "cancel merge");
          self.invoked.store(true, Ordering::SeqCst);
          Err(MockDelegateError::CustomMergeCancelled)
        }
      }
    }
  }

  #[cfg(feature = "nightly")]
  fn ack_payload<'a>(&'a self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send + 'a {
    async move {}
  }

  #[cfg(feature = "nightly")]
  fn notify_ping_complete<'a>(
    &'a self,
    _node: Arc<Node>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
  }

  #[inline]
  fn disable_reliable_pings(&self, _node: &NodeId) -> bool {
    false
  }
}
