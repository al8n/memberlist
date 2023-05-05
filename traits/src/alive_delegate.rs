use showbiz_types::Node;

/// Used to involve a client in processing
/// a node "alive" message. When a node joins, either through
/// a UDP gossip or TCP push/pull, we update the state of
/// that node via an alive message. This can be used to filter
/// a node out and prevent it from being considered a peer
/// using application specific logic.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait AliveDelegate {
  type Error: std::error::Error;

  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error>;

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_alive(&self, peer: &Node) -> Result<(), Self::Error>;
}
