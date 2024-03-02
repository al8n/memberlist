use std::time::Instant;

use smallvec_wrapper::OneOrMore;

use super::Message;

/// The packet receives from the unreliable connection.
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Packet<I, A> {
  /// The raw contents of the packet.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the messages of the packet")
    ),
    setter(attrs(doc = "Sets the messages of the packet (Builder pattern)"))
  )]
  messages: OneOrMore<Message<I, A>>,

  /// Address of the peer. This is an actual address so we
  /// can expose some concrete details about incoming packets.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the address sent the packet")
    ),
    setter(attrs(doc = "Sets the address who sent the packet (Builder pattern)"))
  )]
  from: A,

  /// The time when the packet was received. This should be
  /// taken as close as possible to the actual receipt time to help make an
  /// accurate RTT measurement during probes.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the instant when the packet was received")
    ),
    setter(attrs(doc = "Sets the instant when the packet was received (Builder pattern)"))
  )]
  timestamp: Instant,
}

impl<I, A> Packet<I, A> {
  /// Create a new packet
  #[inline]
  pub const fn new(messages: OneOrMore<Message<I, A>>, from: A, timestamp: Instant) -> Self {
    Self {
      messages,
      from,
      timestamp,
    }
  }

  /// Returns the raw contents of the packet.
  #[inline]
  pub fn into_components(self) -> (OneOrMore<Message<I, A>>, A, Instant) {
    (self.messages, self.from, self.timestamp)
  }

  /// Sets the address who sent the packet
  #[inline]
  pub fn set_from(&mut self, from: A) -> &mut Self {
    self.from = from;
    self
  }

  /// Sets the instant when the packet was received
  #[inline]
  pub fn set_timestamp(&mut self, timestamp: Instant) -> &mut Self {
    self.timestamp = timestamp;
    self
  }

  /// Sets the messages of the packet
  #[inline]
  pub fn set_messages(&mut self, messages: OneOrMore<Message<I, A>>) -> &mut Self {
    self.messages = messages;
    self
  }
}
