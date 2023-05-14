use showbiz_types::Node;

/// Used to inform a client that
/// a node has attempted to join which would result in a
/// name conflict. This happens if two clients are configured
/// with the same name but different addresses.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait ConflictDelegate: Send + Sync + 'static {
  /// Invoked when a name conflict is detected
  #[cfg(not(feature = "async"))]
  fn notify_conflict(&self, existing: bool, other: &Node);

  /// Invoked when a name conflict is detected
  #[cfg(feature = "async")]
  async fn notify_conflict(&self, existing: bool, other: &Node);
}

/// No-op implementation of [`ConflictDelegate`]
#[derive(Debug, Default, Clone, Copy)]
pub struct VoidConflictDelegate;

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl ConflictDelegate for VoidConflictDelegate {
  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn notify_conflict(&self, _existing: bool, _other: &Node) {}

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn notify_conflict(&self, _existing: bool, _other: &Node) {}
}
