use showbiz_types::Node;

/// A simpler delegate that is used only to receive
/// notifications about members joining and leaving. The methods in this
/// delegate may be called by multiple goroutines, but never concurrently.
/// This allows you to reason about ordering.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait EventDelegate {
  /// Invoked when a node is detected to have joined the cluster
  #[cfg(not(feature = "async"))]
  fn notify_join(&self, node: &Node);

  /// Invoked when a node is detected to have joined the cluster
  #[cfg(feature = "async")]
  async fn notify_join(&self, node: &Node);

  /// Invoked when a node is detected to have left the cluster
  #[cfg(not(feature = "async"))]
  fn notify_leave(&self, node: &Node);

  /// Invoked when a node is detected to have left the cluster
  #[cfg(feature = "async")]
  async fn notify_leave(&self, node: &Node);

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(not(feature = "async"))]
  fn notify_update(&self, node: &Node);

  /// Invoked when a node is detected to have
  /// updated, usually involving the meta data.
  #[cfg(feature = "async")]
  async fn notify_update(&self, node: &Node);
}
