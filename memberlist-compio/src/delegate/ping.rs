//! `PingDelegate` — ping-complete observation hook.

use std::time::Duration;

use bytes::Bytes;

// `async fn` in trait is intentional: compio is `!Send`-first, so we want
// the bare-future desugaring, not `-> impl Future + Send`.
#[allow(async_fn_in_trait)]
/// Hook for ping-completion observation.
pub trait PingDelegate: 'static {
  /// Node identifier type.
  type Id;
  /// Address type.
  type Address;

  /// Called when a ping round-trip completes.
  async fn notify_ping_complete(
    &self,
    peer_id: &Self::Id,
    peer_addr: &Self::Address,
    rtt: Duration,
    payload: Bytes,
  ) {
    let _ = (peer_id, peer_addr, rtt, payload); // Unused: default no-op; override to handle.
  }
}
