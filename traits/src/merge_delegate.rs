use showbiz_types::Node;

/// Used to involve a client in
/// a potential cluster merge operation. Namely, when
/// a node does a TCP push/pull (as part of a join),
/// the delegate is involved and allowed to cancel the join
/// based on custom logic. The merge delegate is NOT invoked
/// as part of the push-pull anti-entropy.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait MergeDelegate {
  type Error: std::error::Error;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(not(feature = "async"))]
  fn notify_merge(&self, peers: &[Node]) -> Result<(), Self::Error>;

  /// Invoked when a merge could take place.
  /// Provides a list of the nodes known by the peer.
  #[cfg(feature = "async")]
  async fn notify_merge(&self, peers: &[Node]) -> Result<(), Self::Error>;
}
