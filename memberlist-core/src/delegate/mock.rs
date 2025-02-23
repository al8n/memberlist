use super::*;
use futures::lock::Mutex;
use smallvec_wrapper::TinyVec;

struct MockDelegateInner<I, A> {
  meta: Meta,
  msgs: Vec<Bytes>,
  broadcasts: TinyVec<Bytes>,
  state: Bytes,
  remote_state: Bytes,
  _marker: std::marker::PhantomData<(I, A)>,
}

impl<I, A> Default for MockDelegateInner<I, A> {
  fn default() -> Self {
    Self {
      meta: Meta::empty(),
      msgs: vec![],
      broadcasts: TinyVec::new(),
      state: Bytes::new(),
      remote_state: Bytes::new(),
      _marker: std::marker::PhantomData,
    }
  }
}

#[doc(hidden)]
pub struct MockDelegate<I, A> {
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

  pub fn with_meta(meta: Meta) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        meta,
        ..Default::default()
      })),
    }
  }

  pub fn with_state(state: Bytes) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        state,
        ..Default::default()
      })),
    }
  }

  pub fn with_state_and_broadcasts(state: Bytes, broadcasts: TinyVec<Bytes>) -> Self {
    Self {
      inner: Arc::new(Mutex::new(MockDelegateInner {
        state,
        broadcasts,
        ..Default::default()
      })),
    }
  }
}

impl<I, A> MockDelegate<I, A> {
  pub async fn set_meta(&self, meta: Meta) {
    self.inner.lock().await.meta = meta;
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

impl<I, A> NodeDelegate for MockDelegate<I, A>
where
  I: Send + Sync + 'static,
  A: CheapClone + Send + Sync + 'static,
{
  async fn node_meta(&self, _limit: usize) -> Meta {
    self.inner.lock().await.meta.clone()
  }

  async fn notify_message(&self, msg: Cow<'_, [u8]>) {
    self.inner.lock().await.msgs.push(Bytes::from(msg.to_vec()));
  }

  async fn broadcast_messages<F>(
    &self,
    _limit: usize,
    _encoded_len: F,
  ) -> impl Iterator<Item = Bytes> + Send
  where
    F: Fn(Bytes) -> (usize, Bytes),
  {
    let mut mu = self.inner.lock().await;
    let mut out = TinyVec::new();
    core::mem::swap(&mut out, &mut mu.broadcasts);
    out.into_iter()
  }

  async fn local_state(&self, _join: bool) -> Bytes {
    self.inner.lock().await.state.clone()
  }

  async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
    self.inner.lock().await.remote_state = Bytes::copy_from_slice(buf);
  }
}
