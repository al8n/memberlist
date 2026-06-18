use super::*;

macro_rules! bail_ping_typed {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct $name<I, A> {
      /// The sequence number of the ack
      sequence_number: u32,

      /// Source target, used for a direct reply
      source: Node<I, A>,

      /// [`Node`] is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      target: Node<I, A>,
    }

    impl<I, A> $name<I, A> {
      /// Create a new message
      #[inline(always)]
      pub const fn new(sequence_number: u32, source: Node<I, A>, target: Node<I, A>) -> Self {
        Self {
          sequence_number,
          source,
          target,
        }
      }

      /// Returns the sequence number of the message
      #[inline(always)]
      pub const fn sequence_number(&self) -> u32 {
        self.sequence_number
      }

      /// Returns the source node of the message
      #[inline(always)]
      pub const fn source_ref(&self) -> &Node<I, A> {
        &self.source
      }

      /// Returns the target node of the message
      #[inline(always)]
      pub const fn target_ref(&self) -> &Node<I, A> {
        &self.target
      }

      /// Sets the sequence number of the message
      #[inline(always)]
      pub const fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
        self.sequence_number = sequence_number;
        self
      }

      /// Sets the sequence number of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub const fn with_sequence_number(mut self, sequence_number: u32) -> Self {
        self.sequence_number = sequence_number;
        self
      }

      /// Sets the source node of the message
      #[inline(always)]
      pub fn set_source(&mut self, source: Node<I, A>) -> &mut Self {
        self.source = source;
        self
      }

      /// Sets the source node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_source(mut self, source: Node<I, A>) -> Self {
        self.source = source;
        self
      }

      /// Sets the target node of the message
      #[inline(always)]
      pub fn set_target(&mut self, target: Node<I, A>) -> &mut Self {
        self.target = target;
        self
      }

      /// Sets the target node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_target(mut self, target: Node<I, A>) -> Self {
        self.target = target;
        self
      }
    }

    impl<I: CheapClone, A: CheapClone> CheapClone for $name<I, A> {
      fn cheap_clone(&self) -> Self {
        Self {
          sequence_number: self.sequence_number,
          source: self.source.cheap_clone(),
          target: self.target.cheap_clone(),
        }
      }
    }
  };
}

bail_ping_typed!(
  #[doc = "Ping is sent to a target to check if it is alive"]
  Ping
);
bail_ping_typed!(
  #[doc = "IndirectPing is sent to a target to check if it is alive"]
  IndirectPing
);

impl<I, A> From<IndirectPing<I, A>> for Ping<I, A> {
  fn from(ping: IndirectPing<I, A>) -> Self {
    Self {
      sequence_number: ping.sequence_number,
      source: ping.source,
      target: ping.target,
    }
  }
}
