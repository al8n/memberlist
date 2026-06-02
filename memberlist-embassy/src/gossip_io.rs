//! The [`GossipIo`](memberlist_embedded::GossipIo) implementation over an
//! embassy-net [`UdpSocket`].
//!
//! A short-lived view over the gossip socket, rebuilt each engine pump. The
//! engine reads inbound gossip and writes outbound gossip through it without
//! touching embassy-net; the actual stack progress (link RX/TX) the embassy-net
//! `Stack` drives on its own.
//!
//! embassy-net 0.9 exposes only async + `poll_*` UDP methods (no `try_*`), so
//! the non-blocking [`GossipIo`] ops drive `poll_recv_from` / `poll_send_to`
//! with a no-op [`Waker`]: the engine pump is synchronous and re-polls each
//! socket on the next driver tick, so no datagram is lost by not registering a
//! real waker here (the async driver registers the real recv waker around the
//! pump).

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  task::{Context, Poll, Waker},
};

use embassy_net::{IpEndpoint, udp::UdpSocket};
use memberlist_embedded::GossipIo;

/// A [`GossipIo`] view over a single bound gossip [`UdpSocket`].
///
/// memberlist binds one advertise address, so a single socket suffices (no v4/v6
/// split). Built fresh for each engine pump over the already-progressed socket.
pub struct EmbassyGossip<'a>(&'a UdpSocket<'a>);

impl<'a> EmbassyGossip<'a> {
  /// Build the gossip view over the bound `socket`.
  #[inline]
  pub fn new(socket: &'a UdpSocket<'a>) -> Self {
    Self(socket)
  }
}

impl GossipIo for EmbassyGossip<'_> {
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    let mut cx = Context::from_waker(Waker::noop());
    match self.0.poll_recv_from(buf, &mut cx) {
      Poll::Ready(Ok((len, meta))) => Some((meta.endpoint.into(), len)),
      // `RecvError::Truncated` (the only `Poll::Ready(Err)`): an oversized
      // datagram — larger than this buffer (the configured gossip MTU plus
      // encryption overhead) — was already DEQUEUED by embassy-net before the
      // length check, so it is consumed and gone. Surface a zero-length marker
      // (like the smoltcp driver's `Truncated` skip) so the engine's drain loop
      // treats it as nothing to deliver and RE-POLLS for the next datagram,
      // instead of stopping early: one oversized datagram cannot stall the
      // in-budget datagrams queued behind it. The source address is irrelevant
      // for a zero-length frame; `handle_gossip` on an empty slice is a no-op.
      Poll::Ready(Err(_)) => Some((SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0), 0)),
      // No datagram queued: the drain loop ends for this pump.
      Poll::Pending => None,
    }
  }

  fn send(&mut self, bytes: &[u8], dest: SocketAddr) {
    let mut cx = Context::from_waker(Waker::noop());
    // Ignoring the result: gossip is best-effort. A full tx ring (`Poll::Pending`)
    // or any `SendError` (no route, socket not bound, packet too large) drops this
    // datagram and SWIM recovers on the next gossip round — no error is surfaced.
    let _ = self.0.poll_send_to(bytes, IpEndpoint::from(dest), &mut cx);
  }
}
