//! Event stream — the user-facing observation channel.

use flume::r#async::RecvStream;
use futures_util::Stream;
use memberlist_machine::event::Event;
use std::{
  pin::Pin,
  task::{Context, Poll},
};

/// Stream of memberlist events. Constructed via
/// [`Memberlist::events`](crate::Memberlist::events).
///
/// Generic over the wire id / address types `<I, A>`; pinned aliases
/// like [`TcpMemberlist`](crate::TcpMemberlist) instantiate this as
/// `EventStream<SmolStr, SocketAddr>` through the [`Memberlist::events`]
/// signature.
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
/// the membership FSM. A subscriber can miss an event two ways, counted
/// separately: the EventStream queue overflowed
/// ([`Memberlist::events_dropped`](crate::Memberlist::events_dropped)), or the
/// upstream observation channel dropped the event before it reached fan-out
/// ([`Memberlist::observation_dropped`](crate::Memberlist::observation_dropped)).
/// To detect ALL gaps, poll BOTH counters across a window — any increase means
/// events were missed during it. The lock-free
/// [`Memberlist::snapshot`](crate::Memberlist::snapshot) /
/// [`Memberlist::alive_count`](crate::Memberlist::alive_count) /
/// [`Memberlist::member_count`](crate::Memberlist::member_count) read
/// path is unaffected by event loss and remains the authoritative
/// source of current cluster state.
pub struct EventStream<I: 'static, A: 'static> {
  inner: RecvStream<'static, Event<I, A>>,
}

impl<I: 'static, A: 'static> EventStream<I, A> {
  /// Wrap a flume receiver into an `EventStream`.
  ///
  /// Consumes the receiver: the resulting stream lives `'static` and
  /// owns the queue handle.
  pub(crate) fn new(rx: flume::Receiver<Event<I, A>>) -> Self {
    Self {
      inner: rx.into_stream(),
    }
  }
}

impl<I: 'static, A: 'static> Stream for EventStream<I, A> {
  type Item = Event<I, A>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.inner).poll_next(cx)
  }
}
