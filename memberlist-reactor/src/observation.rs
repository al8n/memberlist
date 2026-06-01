//! The observation task: drains machine events off the driver loop, invokes the
//! [`Delegate`](crate::Delegate) hooks, and forwards membership / control events
//! to the [`EventStream`](crate::EventStream).

use std::{
  net::SocketAddr,
  panic::AssertUnwindSafe,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use flume::{Receiver, Sender, TrySendError};
use futures_util::FutureExt;
use memberlist_proto::{CheapClone, event::Event};

use crate::{NodeId, delegate::Delegate, shared::Shared};

/// The application-payload byte count of an event (`UserPacket` /
/// `RemoteStateReceived`) — used by the obs-channel byte backstop and to route
/// app-data to the delegate only (never the event stream, which cannot
/// reconstruct payloads). `None` for membership / control events.
pub(crate) fn observation_payload_bytes<I, A>(ev: &Event<I, A>) -> Option<u64> {
  match ev {
    Event::UserPacket(p) => Some(p.data_ref().len() as u64),
    Event::RemoteStateReceived(r) => Some(r.user_data_ref().len() as u64),
    _ => None,
  }
}

/// Dispatches one event to its matching [`Delegate`] hook. Control-only events
/// (`LeftCluster` / `DecodeError` / `DialRequested` / `ExchangeCompleted`) have
/// no hook.
async fn dispatch<I, D>(delegate: &D, ev: &Event<I, SocketAddr>)
where
  I: NodeId,
  D: Delegate<Id = I, Address = SocketAddr>,
{
  match ev {
    Event::NodeJoined(node) => delegate.notify_join(node.clone()).await,
    Event::NodeLeft(node) => delegate.notify_leave(node.clone()).await,
    Event::NodeUpdated(node) => delegate.notify_update(node.clone()).await,
    Event::NodeConflict(c) => {
      delegate
        .notify_conflict(c.existing_ref().clone(), c.other_ref().clone())
        .await;
    }
    Event::PingCompleted(p) => {
      let node = p.node_ref();
      delegate
        .notify_ping_complete(
          node.id_ref().cheap_clone(),
          node.address_ref().cheap_clone(),
          p.rtt(),
          p.payload_ref().clone(),
        )
        .await;
    }
    Event::UserPacket(pkt) => delegate.notify_user_msg(pkt.data_ref().clone()).await,
    Event::RemoteStateReceived(rs) => {
      delegate
        .merge_remote_state(rs.user_data_ref().clone(), rs.join())
        .await;
    }
    _ => {}
  }
}

/// Runs the observation loop until the driver drops its `obs_tx` (teardown).
pub(crate) async fn observation_task<I, D>(
  obs_rx: Receiver<Event<I, SocketAddr>>,
  delegate: D,
  events_tx: Sender<Event<I, SocketAddr>>,
  shared: Arc<Shared<I>>,
  obs_payload_bytes: Arc<AtomicU64>,
) where
  I: NodeId,
  D: Delegate<Id = I, Address = SocketAddr>,
{
  while let Ok(ev) = obs_rx.recv_async().await {
    // Reclaim the byte-backstop budget this event occupied, before the (possibly
    // slow) delegate hook, so the driver's enqueue side sees it promptly.
    let payload = observation_payload_bytes(&ev);
    if let Some(bytes) = payload {
      obs_payload_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
    // Contain a panicking delegate hook so the task survives and keeps releasing
    // the byte-backstop reservations of events still queued; a dead obs task
    // would strand them and wedge the byte budget against all future app-data.
    if AssertUnwindSafe(dispatch(&delegate, &ev))
      .catch_unwind()
      .await
      .is_err()
    {
      shared.add_observation_dropped(1);
    }
    // App-data reached the delegate above; the event stream carries only
    // membership / control.
    if payload.is_some() {
      continue;
    }
    // Best-effort fan-out to event-stream subscribers; a full queue (a slow
    // subscriber) drops the event and counts it, never blocking the loop.
    if events_tx
      .try_send(ev)
      .is_err_and(|e| matches!(e, TrySendError::Full(_)))
    {
      shared.add_events_dropped(1);
    }
  }
}
