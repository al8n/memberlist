use parking_lot::Mutex;

use super::*;

struct MockDelegateInner {
  meta: Bytes,
  msgs: Vec<Bytes>,
  broadcasts: Vec<Message>,
  state: Bytes,
  remote_state: Bytes,
}

pub struct MockDelegate {
  inner: Arc<Mutex<MockDelegateInner>>,
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
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl Delegate for MockDelegate {
  type Error = VoidDelegateError;

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
  async fn merge_remote_state(&self, buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    self.inner.lock().remote_state = buf;
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
  async fn notify_alive(&self, _peer: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_conflict(
    &self,
    _existing: Arc<Node>,
    _other: Arc<Node>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, _peers: Vec<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(not(feature = "nightly"))]
  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  #[cfg(not(feature = "nightly"))]
  async fn notify_ping_complete(
    &self,
    _node: Arc<Node>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) -> Result<(), Self::Error> {
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
    _peers: Vec<Node>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move { Ok(()) }
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
