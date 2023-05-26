use bytes::Bytes;
use showbiz_types::Name;

/// Something that can be broadcasted via gossip to
/// the memberlist cluster.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait Broadcast: Send + Sync + 'static {
  /// Error type
  type Error: std::error::Error + Send + Sync + 'static;

  /// Returns the name of the broadcast, if any
  fn name(&self) -> &Name;

  /// Checks if enqueuing the current broadcast
  /// invalidates a previous broadcast
  fn invalidates(&self, other: &Self) -> bool;

  /// Returns bytes form of the message
  fn message(&self) -> &Bytes;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  #[cfg(not(feature = "async"))]
  fn finished(&self) -> Result<(), Self::Error>;

  /// Invoked when the message will no longer
  /// be broadcast, either due to invalidation or to the
  /// transmit limit being reached
  #[cfg(feature = "async")]
  async fn finished(&self) -> Result<(), Self::Error>;

  /// Indicates that each message is
  /// intrinsically unique and there is no need to scan the broadcast queue for
  /// duplicates.
  fn is_unique(&self) -> bool;
}
