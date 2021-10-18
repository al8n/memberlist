use std::time::Instant;

use memberlist_utils::OneOrMore;

use super::Message;

#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Packet<I, A> {
  /// The raw contents of the packet.
  #[viewit(getter(skip))]
  buf: OneOrMore<Message<I, A>>,

  /// Address of the peer. This is an actual address so we
  /// can expose some concrete details about incoming packets.
  from: A,

  /// The time when the packet was received. This should be
  /// taken as close as possible to the actual receipt time to help make an
  /// accurate RTT measurement during probes.
  timestamp: Instant,
}

impl<I, A> Packet<I, A> {
  #[inline]
  pub fn new(buf: OneOrMore<Message<I, A>>, from: A, timestamp: Instant) -> Self {
    Self {
      buf,
      from,
      timestamp,
    }
  }

  #[inline]
  pub fn into_components(self) -> (OneOrMore<Message<I, A>>, A, Instant) {
    (self.buf, self.from, self.timestamp)
  }
}
