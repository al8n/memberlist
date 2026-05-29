//! `NodeDelegate` — user messages and merge-state receive hook.

use std::borrow::Cow;

// `async fn` in trait is intentional: compio is `!Send`-first, so we want
// the bare-future desugaring, not `-> impl Future + Send`.
#[allow(async_fn_in_trait)]
/// Hooks for incoming user messages and received push/pull state.
pub trait NodeDelegate: 'static {
  /// Called when an incoming user message is received.
  async fn notify_user_msg(&self, msg: Cow<'_, [u8]>) {
    let _ = msg; // Unused: default no-op; override to handle.
  }

  /// Called when a remote state payload is received during push/pull.
  async fn merge_remote_state(&self, buf: &[u8], join: bool) {
    let _ = (buf, join); // Unused: default no-op; override to handle.
  }
}
