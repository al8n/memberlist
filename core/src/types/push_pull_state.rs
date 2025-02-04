use std::sync::Arc;

use super::*;

use bytes::Bytes;
use nodecraft::{CheapClone, Node};

/// Push node state is the state push to the remote server.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushNodeState<I, A> {
  /// The id of the push node state.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the id of the push node state")
    ),
    setter(attrs(doc = "Sets the id of the push node state (Builder pattern)"))
  )]
  id: I,
  /// The address of the push node state.
  #[viewit(
    getter(
      const,
      rename = "address",
      style = "ref",
      attrs(doc = "Returns the address of the push node state")
    ),
    setter(
      rename = "with_address",
      attrs(doc = "Sets the address of the push node state (Builder pattern)")
    )
  )]
  addr: A,
  /// Metadata from the delegate for this push node state.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the meta of the push node state")
    ),
    setter(attrs(doc = "Sets the meta of the push node state (Builder pattern)"))
  )]
  meta: Meta,
  /// The incarnation of the push node state.
  #[viewit(
    getter(const, attrs(doc = "Returns the incarnation of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the incarnation of the push node state (Builder pattern)")
    )
  )]
  incarnation: u32,
  /// The state of the push node state.
  #[viewit(
    getter(const, attrs(doc = "Returns the state of the push node state")),
    setter(
      const,
      attrs(doc = "Sets the state of the push node state (Builder pattern)")
    )
  )]
  state: State,
  /// The protocol version of the push node state is speaking.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the protocol version of the push node state is speaking")
    ),
    setter(
      const,
      attrs(
        doc = "Sets the protocol version of the push node state is speaking (Builder pattern)"
      )
    )
  )]
  protocol_version: ProtocolVersion,
  /// The delegate version of the push node state is speaking.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the delegate version of the push node state is speaking")
    ),
    setter(
      const,
      attrs(
        doc = "Sets the delegate version of the push node state is speaking (Builder pattern)"
      )
    )
  )]
  delegate_version: DelegateVersion,
}

impl<I, A> PushNodeState<I, A> {
  /// Construct a new push node state with the given id, address and state.
  #[inline]
  pub const fn new(incarnation: u32, id: I, addr: A, state: State) -> Self {
    Self {
      id,
      addr,
      meta: Meta::empty(),
      incarnation,
      state,
      protocol_version: ProtocolVersion::V1,
      delegate_version: DelegateVersion::V1,
    }
  }

  /// Sets the id of the push node state
  #[inline]
  pub fn set_id(&mut self, id: I) -> &mut Self {
    self.id = id;
    self
  }

  /// Sets the address of the push node state
  #[inline]
  pub fn set_address(&mut self, addr: A) -> &mut Self {
    self.addr = addr;
    self
  }

  /// Sets the meta of the push node state
  #[inline]
  pub fn set_meta(&mut self, meta: Meta) -> &mut Self {
    self.meta = meta;
    self
  }

  /// Sets the incarnation of the push node state
  #[inline]
  pub fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
    self.incarnation = incarnation;
    self
  }

  /// Sets the state of the push node state
  #[inline]
  pub fn set_state(&mut self, state: State) -> &mut Self {
    self.state = state;
    self
  }

  /// Sets the protocol version of the push node state
  #[inline]
  pub fn set_protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
    self.protocol_version = protocol_version;
    self
  }

  /// Sets the delegate version of the push node state
  #[inline]
  pub fn set_delegate_version(&mut self, delegate_version: DelegateVersion) -> &mut Self {
    self.delegate_version = delegate_version;
    self
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for PushNodeState<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      id: self.id.cheap_clone(),
      addr: self.addr.cheap_clone(),
      meta: self.meta.clone(),
      incarnation: self.incarnation,
      state: self.state,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}

impl<I: CheapClone, A: CheapClone> PushNodeState<I, A> {
  /// Returns a [`Node`] with the same id and address as this [`PushNodeState`].
  pub fn node(&self) -> Node<I, A> {
    Node::new(self.id.cheap_clone(), self.addr.cheap_clone())
  }
}

/// Push pull message.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PushPull<I, A> {
  /// Whether the push pull message is a join message.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns whether the push pull message is a join message")
    ),
    setter(
      const,
      attrs(doc = "Sets whether the push pull message is a join message (Builder pattern)")
    )
  )]
  join: bool,
  /// The states of the push pull message.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the states of the push pull message")
    ),
    setter(attrs(doc = "Sets the states of the push pull message (Builder pattern)"))
  )]
  states: Arc<TinyVec<PushNodeState<I, A>>>,
  /// The user data of the push pull message.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the user data of the push pull message")
    ),
    setter(attrs(doc = "Sets the user data of the push pull message (Builder pattern)"))
  )]
  user_data: Bytes,
}

impl<I, A> Clone for PushPull<I, A> {
  fn clone(&self) -> Self {
    Self {
      join: self.join,
      states: self.states.clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> CheapClone for PushPull<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      join: self.join,
      states: self.states.cheap_clone(),
      user_data: self.user_data.clone(),
    }
  }
}

impl<I, A> PushPull<I, A> {
  /// Create a new [`PushPull`] message.
  #[inline]
  pub fn new(join: bool, states: TinyVec<PushNodeState<I, A>>) -> Self {
    Self {
      states: Arc::new(states),
      user_data: Bytes::new(),
      join,
    }
  }

  /// Consumes the [`PushPull`] and returns the states and user data.
  #[inline]
  pub fn into_components(self) -> (bool, Bytes, Arc<TinyVec<PushNodeState<I, A>>>) {
    (self.join, self.user_data, self.states)
  }
}

#[cfg(test)]
const _: () = {
  use std::net::SocketAddr;

  use rand::{distr::Alphanumeric, random, rng, Rng};
  use smol_str::SmolStr;

  impl PushNodeState<SmolStr, SocketAddr> {
    fn generate(size: usize) -> Self {
      Self {
        id: String::from_utf8(
          rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .collect::<Vec<_>>(),
        )
        .unwrap()
        .into(),
        addr: SocketAddr::from(([127, 0, 0, 1], rng().random_range(0..65535))),
        meta: (0..size)
          .map(|_| random::<u8>())
          .collect::<Vec<_>>()
          .try_into()
          .unwrap(),
        incarnation: random(),
        state: State::try_from(rng().random_range(0..=3)).unwrap(),
        protocol_version: ProtocolVersion::V1,
        delegate_version: DelegateVersion::V1,
      }
    }
  }

  impl PushPull<SmolStr, SocketAddr> {
    pub(crate) fn generate(size: usize) -> Self {
      let states = (0..size)
        .map(|_| PushNodeState::generate(size))
        .collect::<TinyVec<_>>();
      let user_data = (0..size).map(|_| random::<u8>()).collect::<Vec<_>>().into();
      let join = random();
      Self {
        join,
        states: Arc::new(states),
        user_data,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use super::*;

  // #[test]
  // fn test_push_server_state() {
  //   for i in 0..100 {
  //     let state = PushNodeState::generate(i);
  //     let mut buf = vec![0; state.encoded_len()];
  //     let encoded_len = state.encode(&mut buf).unwrap();
  //     assert_eq!(encoded_len, state.encoded_len());
  //     let (readed, decoded) = PushNodeState::decode(&buf).unwrap();
  //     assert_eq!(readed, encoded_len);
  //     assert_eq!(decoded, state);
  //   }
  // }

  // #[test]
  // fn test_push_pull() {
  //   for i in 0..100 {
  //     let push_pull = PushPull::generate(i);
  //     let mut buf = vec![0; push_pull.encoded_len()];
  //     let encoded_len = push_pull.encode(&mut buf).unwrap();
  //     assert_eq!(encoded_len, push_pull.encoded_len());
  //     let (readed, decoded) = PushPull::decode(&buf).unwrap();
  //     assert_eq!(readed, encoded_len);
  //     assert_eq!(decoded, push_pull);
  //   }
  // }

  #[test]
  fn test_push_pull_clone_and_cheap_clone() {
    let push_pull = PushPull::generate(100);
    let cloned = push_pull.clone();
    let cheap_cloned = push_pull.cheap_clone();
    assert_eq!(cloned, push_pull);
    assert_eq!(cheap_cloned, push_pull);
    let cloned1 = format!("{:?}", cloned);
    let cheap_cloned1 = format!("{:?}", cheap_cloned);
    assert_eq!(cloned1, cheap_cloned1);
  }

  #[test]
  fn test_push_node_state() {
    let mut state = PushNodeState::generate(100);
    state.set_id("test".into());
    assert_eq!(state.id(), "test");
    state.set_address(SocketAddr::from(([127, 0, 0, 1], 8080)));
    assert_eq!(state.address(), &SocketAddr::from(([127, 0, 0, 1], 8080)));
    state.set_meta(Meta::try_from("test").unwrap());
    assert_eq!(state.meta(), &Meta::try_from("test").unwrap());
    state.set_incarnation(100);
    assert_eq!(state.incarnation(), 100);

    state.set_state(State::Alive);
    assert_eq!(state.state(), State::Alive);

    state.set_protocol_version(ProtocolVersion::V1);
    assert_eq!(state.protocol_version(), ProtocolVersion::V1);

    state.set_delegate_version(DelegateVersion::V1);
    assert_eq!(state.delegate_version(), DelegateVersion::V1);

    let _cloned = state.cheap_clone();
  }
}
