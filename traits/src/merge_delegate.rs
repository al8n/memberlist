use showbiz_types::Node;

/// Used to involve a client in
/// a potential cluster merge operation. Namely, when
/// a node does a TCP push/pull (as part of a join),
/// the delegate is involved and allowed to cancel the join
/// based on custom logic. The merge delegate is NOT invoked
/// as part of the push-pull anti-entropy.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait MergeDelegate: Send + Sync + 'static {
  type Error: std::error::Error + Send + Sync + 'static;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(not(feature = "async"))]
  fn notify_merge(&self, peers: &[Node]) -> Result<(), Self::Error>;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(feature = "async")]
  async fn notify_merge(&self, peers: &[Node]) -> Result<(), Self::Error>;
}

/// No-op implementation of [`MergeDelegate`]
#[derive(Debug, Clone, Copy)]
pub struct VoidMergeDelegate<E: std::error::Error + Send + Sync + 'static>(
  std::marker::PhantomData<E>,
);

impl<E: std::error::Error + Send + Sync + 'static> Default for VoidMergeDelegate<E> {
  fn default() -> Self {
    Self(std::marker::PhantomData)
  }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<E: std::error::Error + Send + Sync + 'static> MergeDelegate for VoidMergeDelegate<E> {
  type Error = E;

  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn notify_merge(&self, _peers: &[Node]) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn notify_merge(&self, _peers: &[Node]) -> Result<(), Self::Error> {
    Ok(())
  }
}
