use showbiz_types::Node;

/// Used to inform a client that
/// a node has attempted to join which would result in a
/// name conflict. This happens if two clients are configured
/// with the same name but different addresses.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ConflictDelegate {
  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_conflict(&self, existing: bool, other: &Node);

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_conflict(&self, existing: bool, other: &Node);
}
