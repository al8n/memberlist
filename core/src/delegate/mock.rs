use futures::lock::Mutex;

use super::*;

struct MockDelegateInner<I, A> {
  meta: Bytes,
  msgs: Vec<Bytes>,
  broadcasts: SmallVec<Bytes>,
  state: Bytes,
  remote_state: Bytes,
  _marker: std::marker::PhantomData<(I, A)>,
}

impl<I, A> Default for MockDelegateInner<I, A> {
  fn default() -> Self {
    Self {
      meta: Bytes::new(),
      msgs: vec![],
      broadcasts: SmallVec::new(),
      state: Bytes::new(),
      remote_state: Bytes::new(),
      _marker: std::marker::PhantomData,
    }
  }
}

pub(crate) struct MockDelegate<I, A> {
  inner: Arc<Mutex<MockDelegateInner<I, A>>>,
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
    }
  }

  pub fn with_meta(meta: Bytes) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        meta,
        ..Default::default()
      })),
    }
  }
}

impl<I, A> MockDelegate<I, A> {
  pub async fn set_meta(&self, meta: Bytes) {
    self.inner.lock().await.meta = meta;
  }

  pub async fn set_state(&self, state: Bytes) {
    self.inner.lock().await.state = state;
  }

  pub async fn set_broadcasts(&self, broadcasts: SmallVec<Bytes>) {
    self.inner.lock().await.broadcasts = broadcasts;
  }

  pub async fn get_remote_state(&self) -> Bytes {
    self.inner.lock().await.remote_state.clone()
  }

  pub async fn get_messages(&self) -> Vec<Bytes> {
    let mut mu = self.inner.lock().await;
    let mut out = vec![];
    core::mem::swap(&mut out, &mut mu.msgs);
    out
  }
}

impl<I: Id, A> NodeDelegate for MockDelegate<I, A>
where
  A: CheapClone + Send + Sync + 'static,
{
  type Address = A;
  type Id = I;

  async fn node_meta(&self, _limit: usize) -> Bytes {
    self.inner.lock().await.meta.clone()
  }

  async fn notify_message(&self, msg: Bytes) {
    self.inner.lock().await.msgs.push(msg);
  }

  async fn broadcast_messages<F>(
    &self,
    _overhead: usize,
    _limit: usize,
    _encoded_len: F,
  ) -> SmallVec<Bytes>
  where
    F: Fn(Bytes) -> (usize, Bytes),
  {
    let mut mu = self.inner.lock().await;
    let mut out = SmallVec::new();
    core::mem::swap(&mut out, &mut mu.broadcasts);
    out
  }

  async fn local_state(&self, _join: bool) -> Bytes {
    self.inner.lock().await.state.clone()
  }

  async fn merge_remote_state(&self, buf: Bytes, _join: bool) {
    self.inner.lock().await.remote_state = buf;
  }
}
