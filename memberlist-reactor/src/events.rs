//! [`EventStream`] — a subscriber's view of membership / control events.

use std::{
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll},
};

use futures_util::Stream;
use memberlist_machine::event::Event;

/// A stream of membership / control [`Event`]s for one subscriber, obtained from
/// `Memberlist::events`.
///
/// Backed by a shared MPMC queue: events round-robin across subscribers rather
/// than broadcast, so the common single-consumer case observes every event
/// (multiple consumers each see a disjoint subset — layer your own fan-out if
/// you need broadcast). Application data (`UserPacket` / `RemoteStateReceived`)
/// is delivered to the [`Delegate`](crate::Delegate) only, never here.
pub struct EventStream<I: 'static, A: 'static = SocketAddr> {
  inner: flume::r#async::RecvStream<'static, Event<I, A>>,
}

impl<I: 'static, A: 'static> EventStream<I, A> {
  /// Wraps a flume receiver; the resulting stream is `'static` and owns the
  /// queue handle.
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
