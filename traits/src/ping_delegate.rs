use bytes::Bytes;
use showbiz_types::Node;

/// Used to notify an observer how long it took for a ping message to
/// complete a round trip.  It can also be used for writing arbitrary byte slices
/// into ack messages. Note that in order to be meaningful for RTT estimates, this
/// delegate does not apply to indirect pings, nor fallback pings sent over TCP.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait PingDelegate {
  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(not(feature = "async"))]
  fn ack_payload(&self) -> Bytes;

  /// Invoked when an ack is being sent; the returned bytes will be appended to the ack
  #[cfg(feature = "async")]
  async fn ack_payload(&self) -> Bytes;

  /// Invoked when an ack for a ping is received
  #[cfg(not(feature = "async"))]
  fn notify_ping_complete(&self, node: &Node, rtt: std::time::Duration, payload: Bytes);

  /// Invoked when an ack for a ping is received
  #[cfg(feature = "async")]
  async fn notify_ping_complete(&self, node: &Node, rtt: std::time::Duration, payload: Bytes);
}
