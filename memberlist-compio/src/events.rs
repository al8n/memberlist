//! Event stream — the user-facing observation channel.

use flume::r#async::RecvStream;
use futures_util::Stream;
use memberlist_machine::event::Event;
use smol_str::SmolStr;
use std::{
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
};

/// Stream of memberlist events. Constructed via
/// [`Memberlist::events`](crate::Memberlist::events).
///
/// **Concurrency model:** flume MPMC — multiple `events()` calls each
/// return an independent `EventStream`, but events ROUND-ROBIN between
/// subscribers (NOT broadcast). For single-consumer (one task observes
/// all events) this is the right shape. For multi-consumer broadcast,
/// wrap with an explicit broadcast layer.
///
/// **Lossy under backpressure:** the events channel is bounded (1024
/// at construction). When the queue is full the driver drops the
/// newest event rather than block — a slow subscriber must not stall
/// the membership FSM. Subscribers that need to detect dropped events
/// can poll [`Memberlist::events_dropped`](crate::Memberlist::events_dropped):
/// a monotonic increase across a window means the driver dropped at
/// least that many events during the window. The lock-free
/// [`Memberlist::snapshot`](crate::Memberlist::snapshot) /
/// [`Memberlist::alive_count`](crate::Memberlist::alive_count) /
/// [`Memberlist::member_count`](crate::Memberlist::member_count) read
/// path is unaffected by event loss and remains the authoritative
/// source of current cluster state.
pub struct EventStream {
  inner: RecvStream<'static, Event<SmolStr, SocketAddr>>,
}

impl EventStream {
  /// Wrap a flume receiver into an `EventStream`.
  ///
  /// Consumes the receiver: the resulting stream lives `'static` and
  /// owns the queue handle.
  pub(crate) fn new(rx: flume::Receiver<Event<SmolStr, SocketAddr>>) -> Self {
    Self {
      inner: rx.into_stream(),
    }
  }
}

impl Stream for EventStream {
  type Item = Event<SmolStr, SocketAddr>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.inner).poll_next(cx)
  }
}
