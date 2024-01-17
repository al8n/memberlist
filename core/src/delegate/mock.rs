use parking_lot::Mutex;
use std::{
  sync::atomic::{AtomicBool, AtomicUsize},
  time::Duration,
};

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
struct MockDelegateInner<I, A> {
  meta: Bytes,
  msgs: Vec<Bytes>,
  broadcasts: Vec<Bytes>,
  state: Bytes,
  remote_state: Bytes,
  conflict_existing: Option<Arc<Server<I, A>>>,
  conflict_other: Option<Arc<Server<I, A>>>,
  ping_other: Option<Arc<Server<I, A>>>,
  ping_rtt: Duration,
  ping_payload: Bytes,
}

pub struct MockDelegate<I, A> {
  inner: Arc<Mutex<MockDelegateInner<I, A>>>,
  invoked: Arc<AtomicBool>,
  ty: MockDelegateType,
  ignore: Option<I>,
  count: Arc<AtomicUsize>,
}

impl<I, A> Default for MockDelegate<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> MockDelegate<I, A> {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::None,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn cancel_merge() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::CancelMerge,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn is_invoked(&self) -> bool {
    self.invoked.load(atomic::Ordering::SeqCst)
  }

  pub fn cancel_alive(ignore: I) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::CancelAlive,
      ignore: Some(ignore),
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn notify_conflict() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::NotifyConflict,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn ping() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::NotifyConflict,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn user_data() -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner::default())),
      invoked: Arc::new(AtomicBool::new(false)),
      ty: MockDelegateType::UserData,
      ignore: None,
      count: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn count(&self) -> usize {
    self.count.load(atomic::Ordering::SeqCst)
  }
}

impl<I, A> MockDelegate<I, A> {
  pub fn set_meta(&self, meta: Bytes) {
    self.inner.lock().meta = meta;
  }

  pub fn set_state(&self, state: Bytes) {
    self.inner.lock().state = state;
  }

  pub fn set_broadcasts(&self, broadcasts: Vec<Message<I, A>>) {
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

  pub fn get_contents(&self) -> Option<(Arc<Server<I, A>>, Duration, Bytes)> {
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

impl<I: Id, A: Address> Delegate for MockDelegate<I, A> {
  type Error = MockDelegateError;
  type Address = A;
  type Id = I;

  fn node_meta(&self, _limit: usize) -> Bytes {
    self.inner.lock().meta.clone()
  }

  async fn notify_message(&self, msg: Bytes) -> Result<(), Self::Error> {
    self.inner.lock().msgs.push(msg);
    Ok(())
  }

  async fn broadcast_messages<F>(
    &self,
    _overhead: usize,
    _limit: usize,
    _encoded_len: F,
  ) -> Result<Vec<Bytes>, Self::Error>
  where
    F: Fn(Bytes) -> (usize, Bytes),
  {
    let mut mu = self.inner.lock();
    let mut out = vec![];
    core::mem::swap(&mut out, &mut mu.broadcasts);
    Ok(out)
  }

  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(self.inner.lock().state.clone())
  }

  async fn merge_remote_state(&self, buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    self.inner.lock().remote_state = buf.to_owned().into();
    Ok(())
  }

  async fn notify_join(&self, _node: Arc<Server<I, A>>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_leave(&self, _node: Arc<Server<I, A>>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_update(&self, _node: Arc<Server<I, A>>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_alive(&self, peer: Arc<Server<I, A>>) -> Result<(), Self::Error> {
    match self.ty {
      MockDelegateType::CancelAlive => {
        self.count.fetch_add(1, atomic::Ordering::SeqCst);
        if let Some(ignore) = &self.ignore {
          if peer.id() == ignore {
            return Ok(());
          }
        }
        tracing::info!(target:  "showbiz.mock.delegate", "cancel alive");
        Err(MockDelegateError::CustomAliveCancelled)
      }
      _ => Ok(()),
    }
  }

  async fn notify_conflict(
    &self,
    existing: Arc<Server<I, A>>,
    other: Arc<Server<I, A>>,
  ) -> Result<(), Self::Error> {
    if self.ty == MockDelegateType::NotifyConflict {
      let mut inner = self.inner.lock();
      inner.conflict_existing = Some(existing);
      inner.conflict_other = Some(other);
    }

    Ok(())
  }

  async fn notify_merge(&self, _peers: Vec<Arc<Server<I, A>>>) -> Result<(), Self::Error> {
    match self.ty {
      MockDelegateType::CancelMerge => {
        use atomic::Ordering;
        tracing::info!(target:  "showbiz.mock.delegate", "cancel merge");
        self.invoked.store(true, Ordering::SeqCst);
        Err(MockDelegateError::CustomMergeCancelled)
      }
      _ => Ok(()),
    }
  }

  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    if self.ty == MockDelegateType::Ping {
      return Ok(Bytes::from_static(b"whatever"));
    }
    Ok(Bytes::new())
  }

  async fn notify_ping_complete(
    &self,
    node: Arc<Server<I, A>>,
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

  #[inline]
  fn disable_reliable_pings(&self, _node: &Node<I, A>) -> bool {
    false
  }
}
