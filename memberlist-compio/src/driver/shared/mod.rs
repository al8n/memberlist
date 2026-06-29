//! Driver helpers shared by every transport backend's driver loop (the
//! stream driver in [`crate::driver`] and the QUIC driver in
//! [`crate::quic::driver`]).
//!
//! These are the parts of the observation / event hand-off path that are
//! independent of the reliable plane: the cluster-wide [`ExchangeId`] token,
//! the [`Delegate`] hook dispatcher, the cooperative yield used to drain a
//! bounded observation channel, and the observation byte-backstop accounting.
//! They live here — outside the feature-gated [`crate::driver`] /
//! [`crate::bridge`] stream-transport modules — so the QUIC driver can reuse
//! them without depending on the byte-stream plane (which is only compiled with
//! a `tcp` / `tls-*` feature).

use std::cell::Cell;

use memberlist_proto::event::Event;

use crate::{delegate::Delegate, transport::runtime::CidrFilter};
use core::task::Poll;
use smallvec::SmallVec;
use std::net::{IpAddr, SocketAddr};

/// Coordinator-allocated handle for one in-flight reliable exchange.
///
/// The driver and the per-bridge task agree on the same opaque id without
/// the rest of the crate having to name the machine's `streams` module.
/// Sourced from the ungated [`memberlist_proto::event`] module so the QUIC
/// driver — whose reliable plane is `QuicEndpoint`, not the byte-stream
/// `streams` plane — shares the identical id type as the stream backends.
pub(crate) type ExchangeId = memberlist_proto::event::ExchangeId;

/// Fire the matching [`Delegate`] hook for one drained [`Event`].
///
/// The event-shaped hooks (`notify_join` / `notify_leave` / `notify_update`
/// / `notify_ping_complete`) run on the driver thread BEFORE the event is
/// forwarded to subscribers, so a delegate observes the transition before
/// any [`EventStream`](crate::EventStream) consumer does. The membership
/// FSM already carries the resolved `Arc<NodeState>` inside each variant,
/// so the hook borrows it (cheap `Arc` bump) with no re-projection.
///
/// Admission (`notify_alive` / `notify_merge`) is NOT fired here — those
/// are the machine's `AliveDelegate` / `MergeDelegate` predicates, supplied
/// via [`Options`](crate::Options) and run inline inside the FSM ahead of
/// the alive/merge transition. The observation [`Delegate`] is a distinct
/// concern: its hooks observe transitions the FSM has already applied.
pub(crate) async fn dispatch_event_delegate<I, A, D>(delegate: &D, ev: &Event<I, A>)
where
  D: Delegate<Id = I, Address = A>,
{
  match ev {
    Event::NodeJoined(node) => delegate.notify_join(node.clone()).await,
    Event::NodeLeft(node) => delegate.notify_leave(node.clone()).await,
    Event::NodeUpdated(node) => delegate.notify_update(node.clone()).await,
    Event::PingCompleted(payload) => {
      let node = payload.node_ref();
      delegate
        .notify_ping_complete(
          node.id_ref(),
          node.address_ref(),
          payload.rtt(),
          payload.payload_ref().clone(),
        )
        .await;
    }
    Event::NodeConflict(c) => {
      delegate
        .notify_conflict(c.existing_ref().clone(), c.other_ref().clone())
        .await;
    }
    Event::UserPacket(pkt) => {
      delegate
        .notify_user_msg(std::borrow::Cow::Borrowed(pkt.data_ref().as_ref()))
        .await;
    }
    Event::RemoteStateReceived(rs) => {
      delegate
        .merge_remote_state(rs.user_data_ref().as_ref(), rs.join())
        .await;
    }
    _ => {}
  }
}

/// Yield to the runtime exactly once.
///
/// The event drain is synchronous — no `.await` fires for membership
/// events — so on a single-threaded runtime the observation task is not
/// scheduled mid-drain. A bounded `obs_tx` would therefore overflow on a
/// single large-but-valid burst (e.g. a join push/pull carrying many members)
/// before the task drains a single event. Yielding hands the scheduler to the
/// already-woken observation task so it can drain `obs_rx` before the drain
/// continues. Runtime-agnostic (no dependency on a specific `yield_now`):
/// re-arms the waker and returns `Pending` once, so the executor runs other
/// ready tasks before re-polling this one.
pub(crate) async fn yield_once() {
  let mut yielded = false;
  core::future::poll_fn(move |cx| {
    if yielded {
      Poll::Ready(())
    } else {
      yielded = true;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  })
  .await
}

pub(crate) use memberlist_driver::observation_payload_bytes;

/// Add a just-enqueued event's payload weight (if any) to the byte-backstop
/// counter. Paired with the subtract in each driver's `observation_task` on
/// dequeue.
pub(crate) fn add_obs_payload(counter: &Cell<u64>, bytes: Option<u64>) {
  if let Some(b) = bytes {
    counter.set(counter.get() + b);
  }
}

/// Build a fully-resolved join's reply from its reached set and requested count.
/// A non-empty set resolves `Ok(set)`; an empty set is the all-failed case,
/// surfacing `JoinFailed { requested, contacted: 0 }` with an empty
/// reached-so-far set (the partial-success slot mirrored from the serf driver).
pub(crate) fn join_reply(
  contacted: SmallVec<[SocketAddr; 1]>,
  requested: usize,
) -> crate::command::JoinReply {
  if contacted.is_empty() {
    Err((
      SmallVec::new(),
      crate::error::MemberlistError::JoinFailed(crate::error::JoinFailed::new(requested, 0)),
    ))
  } else {
    Ok(contacted)
  }
}

/// CIDR transport filter shared by the stream and QUIC driver loops: whether the
/// policy blocks `ip` (always `false` without the `cidr` feature). Applied at recv
/// (the gossip / QUIC datagram source) and at accept (the reliable stream peer).
#[cfg(feature = "cidr")]
pub(crate) fn cidr_blocks(filter: &CidrFilter, ip: IpAddr) -> bool {
  filter.as_ref().is_some_and(|p| p.is_blocked(&ip))
}
#[cfg(not(feature = "cidr"))]
pub(crate) fn cidr_blocks(_filter: &CidrFilter, _ip: IpAddr) -> bool {
  false
}

#[cfg(test)]
mod tests;
