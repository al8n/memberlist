macro_rules! bad_bail_typed {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct $name<I> {
      /// The incarnation of the message.
      incarnation: u32,
      /// The node of the message.
      node: I,
      /// The source node of the message.
      from: I,
    }

    impl<I> $name<I> {
      /// Create a new message
      #[inline(always)]
      pub const fn new(incarnation: u32, node: I, from: I) -> Self {
        Self {
          incarnation,
          node,
          from,
        }
      }

      /// Returns the incarnation of the message.
      #[inline(always)]
      pub const fn incarnation(&self) -> u32 {
        self.incarnation
      }

      /// Returns the node of the message.
      #[inline(always)]
      pub const fn node_ref(&self) -> &I {
        &self.node
      }

      /// Returns the source node of the message.
      #[inline(always)]
      pub const fn from_ref(&self) -> &I {
        &self.from
      }

      /// Sets the incarnation of the message
      #[inline(always)]
      pub const fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
        self.incarnation = incarnation;
        self
      }

      /// Sets the incarnation of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub const fn with_incarnation(mut self, incarnation: u32) -> Self {
        self.incarnation = incarnation;
        self
      }

      /// Sets the source node of the message
      #[inline(always)]
      pub fn set_from(&mut self, source: I) -> &mut Self {
        self.from = source;
        self
      }

      /// Sets the source node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_from(mut self, source: I) -> Self {
        self.from = source;
        self
      }

      /// Sets the node which in this state
      #[inline(always)]
      pub fn set_node(&mut self, target: I) -> &mut Self {
        self.node = target;
        self
      }

      /// Sets the node of the message (Builder pattern)
      #[must_use]
      #[inline(always)]
      pub fn with_node(mut self, target: I) -> Self {
        self.node = target;
        self
      }
    }
  };
}

bad_bail_typed!(
  /// Suspect message
  Suspect
);
bad_bail_typed!(
  /// Dead message
  Dead
);
