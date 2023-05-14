use bytes::Bytes;
use showbiz_types::Node;

/// Used to notify an observer how long it took for a ping message to
/// complete a round trip.  It can also be used for writing arbitrary byte slices
/// into ack messages. Note that in order to be meaningful for RTT estimates, this
/// delegate does not apply to indirect pings, nor fallback pings sent over TCP.
#[cfg_attr(feature = "async", async_trait::async_trait)]
pub trait PingDelegate: Send + Sync + 'static {
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

/// No-op implementation of [`PingDelegate`]
#[derive(Debug, Default, Clone, Copy)]
pub struct VoidPingDelegate;

#[cfg_attr(feature = "async", async_trait::async_trait)]
impl PingDelegate for VoidPingDelegate {
  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn ack_payload(&self) -> Bytes {
    Bytes::new()
  }

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn ack_payload(&self) -> Bytes {
    Bytes::new()
  }

  #[cfg(not(feature = "async"))]
  #[inline(always)]
  fn notify_ping_complete(&self, _node: &Node, _rtt: std::time::Duration, _payload: Bytes) {}

  #[cfg(feature = "async")]
  #[inline(always)]
  async fn notify_ping_complete(&self, _node: &Node, _rtt: std::time::Duration, _payload: Bytes) {}
}
