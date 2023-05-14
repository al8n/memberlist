use showbiz_types::Node;

/// Used to involve a client in processing
/// a node "alive" message. When a node joins, either through
/// a UDP gossip or TCP push/pull, we update the state of
/// that node via an alive message. This can be used to filter
/// a node out and prevent it from being considered a peer
/// using application specific logic.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AliveDelegate: Send + Sync + 'static {
  type Error: std::error::Error + Send + Sync + 'static;

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error>;
}

/// No-op implementation of [`AliveDelegate`]
#[derive(Debug, Clone, Copy)]
pub struct VoidAliveDelegate<E: std::error::Error + Send + Sync + 'static>(
  std::marker::PhantomData<E>,
);

impl<E: std::error::Error + Send + Sync + 'static> Default for VoidAliveDelegate<E> {
  fn default() -> Self {
    Self(std::marker::PhantomData)
  }
}

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl<E: std::error::Error + Send + Sync + 'static> AliveDelegate for VoidAliveDelegate<E> {
  type Error = E;

  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn notify_alive(&self, _peer: &Node) -> Result<(), Self::Error> {
    Ok(())
  }

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn notify_alive(&self, _peer: &Node) -> Result<(), Self::Error> {
    Ok(())
  }
}
