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

#[cfg(test)]
mod tests {
  use std::{
    future::Future,
    sync::{Arc, Mutex},
  };

  use bytes::Bytes;
  use memberlist_proto::{
    event::{Reliability, UserPacket},
    typed::{NodeState, State},
  };
  use smol_str::SmolStr;

  use super::*;
  use crate::{
    delegate::{Delegate, VoidDelegate},
    snapshot::MemberlistSnapshot,
  };

  fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  fn node(id: &str) -> Arc<NodeState<SmolStr, SocketAddr>> {
    Arc::new(NodeState::new(SmolStr::new(id), sock(1), State::Alive))
  }

  fn user_packet(data: &'static [u8]) -> Event<SmolStr, SocketAddr> {
    Event::UserPacket(UserPacket::new(
      sock(2),
      Bytes::from_static(data),
      Reliability::Unreliable,
    ))
  }

  fn test_shared() -> Arc<Shared<SmolStr>> {
    let local = node("me");
    let snap = MemberlistSnapshot::new(vec![local.clone()], local, 1, 1, 0);
    Arc::new(Shared::new(snap))
  }

  /// A `UserPacket` reports its payload byte length (the byte-backstop counter);
  /// membership / control events report `None`.
  #[test]
  fn observation_payload_bytes_only_for_app_data() {
    assert_eq!(
      observation_payload_bytes(&user_packet(b"hello")),
      Some(5),
      "a user packet reports its payload length"
    );
    assert_eq!(
      observation_payload_bytes::<SmolStr, SocketAddr>(&Event::LeftCluster),
      None,
      "a control event carries no app-data bytes"
    );
    assert_eq!(
      observation_payload_bytes(&Event::NodeJoined(node("a"))),
      None,
      "a membership event carries no app-data bytes"
    );
  }

  /// Recording delegate capturing every hook invocation for the task tests.
  #[derive(Default)]
  struct RecordingDelegate {
    joins: Arc<Mutex<Vec<SmolStr>>>,
    leaves: Arc<Mutex<Vec<SmolStr>>>,
    user_msgs: Arc<Mutex<Vec<Vec<u8>>>>,
  }

  impl Delegate for RecordingDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;

    fn notify_join(
      &self,
      n: Arc<NodeState<SmolStr, SocketAddr>>,
    ) -> impl Future<Output = ()> + Send + '_ {
      let joins = self.joins.clone();
      async move { joins.lock().unwrap().push(n.id_ref().clone()) }
    }

    fn notify_leave(
      &self,
      n: Arc<NodeState<SmolStr, SocketAddr>>,
    ) -> impl Future<Output = ()> + Send + '_ {
      let leaves = self.leaves.clone();
      async move { leaves.lock().unwrap().push(n.id_ref().clone()) }
    }

    fn notify_user_msg(&self, msg: Bytes) -> impl Future<Output = ()> + Send + '_ {
      let user_msgs = self.user_msgs.clone();
      async move { user_msgs.lock().unwrap().push(msg.to_vec()) }
    }
  }

  /// The task dispatches membership events to the matching delegate hook AND
  /// fans them out to the event stream; the loop exits when the obs sender is
  /// dropped.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn task_dispatches_membership_and_fans_out() {
    let (obs_tx, obs_rx) = flume::unbounded::<Event<SmolStr, SocketAddr>>();
    let (events_tx, events_rx) = flume::bounded::<Event<SmolStr, SocketAddr>>(16);
    let shared = test_shared();
    let obs_bytes = Arc::new(AtomicU64::new(0));
    let delegate = RecordingDelegate::default();
    let joins = delegate.joins.clone();
    let leaves = delegate.leaves.clone();

    let task = tokio::spawn(observation_task::<SmolStr, RecordingDelegate>(
      obs_rx,
      delegate,
      events_tx,
      shared.clone(),
      obs_bytes,
    ));

    obs_tx.send(Event::NodeJoined(node("joiner"))).unwrap();
    obs_tx.send(Event::NodeLeft(node("leaver"))).unwrap();
    // Dropping the sender ends the loop after the queued events drain.
    drop(obs_tx);
    task.await.expect("obs task joins cleanly");

    assert_eq!(*joins.lock().unwrap(), vec![SmolStr::new("joiner")]);
    assert_eq!(*leaves.lock().unwrap(), vec![SmolStr::new("leaver")]);
    // Both membership events also fanned out to the event stream.
    let mut streamed = Vec::new();
    while let Ok(ev) = events_rx.try_recv() {
      streamed.push(ev);
    }
    assert_eq!(streamed.len(), 2, "both membership events fanned out");
  }

  /// An app-data event reaches the delegate only (never the event stream), and
  /// the task reclaims its reserved byte-backstop budget on dequeue.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn task_routes_app_data_to_delegate_and_reclaims_bytes() {
    let (obs_tx, obs_rx) = flume::unbounded::<Event<SmolStr, SocketAddr>>();
    let (events_tx, events_rx) = flume::bounded::<Event<SmolStr, SocketAddr>>(16);
    let shared = test_shared();
    // Pre-reserve the bytes the driver would have reserved for the packet below.
    let obs_bytes = Arc::new(AtomicU64::new(5));
    let delegate = RecordingDelegate::default();
    let user_msgs = delegate.user_msgs.clone();

    let task = tokio::spawn(observation_task::<SmolStr, RecordingDelegate>(
      obs_rx,
      delegate,
      events_tx,
      shared.clone(),
      obs_bytes.clone(),
    ));

    obs_tx.send(user_packet(b"hello")).unwrap();
    drop(obs_tx);
    task.await.expect("obs task joins cleanly");

    assert_eq!(
      *user_msgs.lock().unwrap(),
      vec![b"hello".to_vec()],
      "the user payload reached notify_user_msg"
    );
    assert_eq!(
      obs_bytes.load(Ordering::Relaxed),
      0,
      "the obs task reclaimed the reserved payload bytes on dequeue"
    );
    assert!(
      events_rx.try_recv().is_err(),
      "app-data must NOT fan out to the event stream"
    );
  }

  /// A full event stream (a slow subscriber) drops the membership event and
  /// counts it, never blocking the task.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn task_counts_dropped_events_on_full_stream() {
    let (obs_tx, obs_rx) = flume::unbounded::<Event<SmolStr, SocketAddr>>();
    // Capacity-1 stream that we deliberately leave full.
    let (events_tx, events_rx) = flume::bounded::<Event<SmolStr, SocketAddr>>(1);
    let shared = test_shared();
    let obs_bytes = Arc::new(AtomicU64::new(0));

    let task = tokio::spawn(
      observation_task::<SmolStr, VoidDelegate<SmolStr, SocketAddr>>(
        obs_rx,
        VoidDelegate::new(),
        events_tx,
        shared.clone(),
        obs_bytes,
      ),
    );

    // First event fills the capacity-1 stream; the second has nowhere to go.
    obs_tx.send(Event::NodeJoined(node("a"))).unwrap();
    obs_tx.send(Event::NodeJoined(node("b"))).unwrap();
    drop(obs_tx);
    task.await.expect("obs task joins cleanly");

    assert_eq!(
      shared.events_dropped(),
      1,
      "the second event found the stream full and was counted"
    );
    // The stream still holds the first event.
    assert!(events_rx.try_recv().is_ok());
  }
}
