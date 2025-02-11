use quickcheck::{Arbitrary, Gen};
use triomphe::Arc;

use super::{
  Ack, Alive, Dead, DelegateVersion, ErrorResponse, IndirectPing, Label, LabeledMessage, Meta,
  Nack, NodeState, Ping, ProtocolVersion, PushNodeState, PushPull, State, Suspect,
};

impl Arbitrary for Ack {
  fn arbitrary(g: &mut Gen) -> Self {
    Self {
      sequence_number: u32::arbitrary(g),
      payload: Vec::<u8>::arbitrary(g).into(),
    }
  }
}

impl Arbitrary for Nack {
  fn arbitrary(g: &mut Gen) -> Self {
    Self {
      sequence_number: u32::arbitrary(g),
    }
  }
}

impl<I, A> Arbitrary for Alive<I, A>
where
  I: Arbitrary,
  A: Arbitrary,
{
  fn arbitrary(g: &mut quickcheck::Gen) -> Self {
    Self {
      incarnation: Arbitrary::arbitrary(g),
      meta: Arbitrary::arbitrary(g),
      node: Arbitrary::arbitrary(g),
      protocol_version: Arbitrary::arbitrary(g),
      delegate_version: Arbitrary::arbitrary(g),
    }
  }
}

impl<I> Arbitrary for Suspect<I>
where
  I: Arbitrary,
{
  fn arbitrary(g: &mut Gen) -> Self {
    Self {
      incarnation: Arbitrary::arbitrary(g),
      node: Arbitrary::arbitrary(g),
      from: Arbitrary::arbitrary(g),
    }
  }
}

impl<I> Arbitrary for Dead<I>
where
  I: Arbitrary,
{
  fn arbitrary(g: &mut Gen) -> Self {
    Self {
      incarnation: Arbitrary::arbitrary(g),
      node: Arbitrary::arbitrary(g),
      from: Arbitrary::arbitrary(g),
    }
  }
}

impl Arbitrary for ErrorResponse {
  fn arbitrary(g: &mut Gen) -> Self {
    Self::new(String::arbitrary(g))
  }
}

impl<I, A> Arbitrary for PushNodeState<I, A>
where
  I: Arbitrary,
  A: Arbitrary,
{
  fn arbitrary(g: &mut Gen) -> Self {
    Self {
      id: Arbitrary::arbitrary(g),
      addr: Arbitrary::arbitrary(g),
      meta: Arbitrary::arbitrary(g),
      incarnation: Arbitrary::arbitrary(g),
      state: Arbitrary::arbitrary(g),
      protocol_version: Arbitrary::arbitrary(g),
      delegate_version: Arbitrary::arbitrary(g),
    }
  }
}

impl<I, A> Arbitrary for PushPull<I, A>
where
  I: Arbitrary,
  A: Arbitrary,
{
  fn arbitrary(g: &mut Gen) -> Self {
    let states = Vec::<PushNodeState<I, A>>::arbitrary(g);
    let user_data = Vec::<u8>::arbitrary(g).into();
    Self {
      join: Arbitrary::arbitrary(g),
      states: Arc::from(states),
      user_data,
    }
  }
}

impl Arbitrary for Label {
  fn arbitrary(g: &mut Gen) -> Self {
    let mut s = String::new();
    while s.len() < 253 {
      let c = char::arbitrary(g);
      let char_len = c.len_utf8();

      if s.len() + char_len > 253 {
        break;
      }
      s.push(c);
    }

    Label(s.into())
  }
}

impl<I, A> Arbitrary for LabeledMessage<I, A>
where
  I: Arbitrary,
  A: Arbitrary,
{
  fn arbitrary(g: &mut Gen) -> Self {
    Self::new(
      Arbitrary::arbitrary(g),
      <Vec<u8> as Arbitrary>::arbitrary(g).into(),
    )
  }
}

impl Arbitrary for Meta {
  fn arbitrary(g: &mut quickcheck::Gen) -> Self {
    let len = usize::arbitrary(g) % Self::MAX_SIZE;
    let mut buf = Vec::with_capacity(len);
    for _ in 0..len {
      buf.push(u8::arbitrary(g));
    }
    Meta::try_from(buf).unwrap()
  }
}

impl Arbitrary for DelegateVersion {
  fn arbitrary(g: &mut quickcheck::Gen) -> Self {
    u8::arbitrary(g).into()
  }
}

impl Arbitrary for ProtocolVersion {
  fn arbitrary(g: &mut quickcheck::Gen) -> Self {
    u8::arbitrary(g).into()
  }
}

impl<I, A> Arbitrary for NodeState<I, A>
where
  I: Arbitrary,
  A: Arbitrary,
{
  fn arbitrary(g: &mut Gen) -> Self {
    Self {
      id: I::arbitrary(g),
      addr: A::arbitrary(g),
      meta: Meta::arbitrary(g),
      state: State::arbitrary(g),
      protocol_version: ProtocolVersion::arbitrary(g),
      delegate_version: DelegateVersion::arbitrary(g),
    }
  }
}

impl Arbitrary for State {
  fn arbitrary(g: &mut Gen) -> Self {
    u8::arbitrary(g).into()
  }
}

impl<I: Arbitrary, A: Arbitrary> Arbitrary for Ping<I, A> {
  fn arbitrary(g: &mut Gen) -> Self {
    Self::new(
      Arbitrary::arbitrary(g),
      Arbitrary::arbitrary(g),
      Arbitrary::arbitrary(g),
    )
  }
}

impl<I: Arbitrary, A: Arbitrary> Arbitrary for IndirectPing<I, A> {
  fn arbitrary(g: &mut Gen) -> Self {
    Self::new(
      Arbitrary::arbitrary(g),
      Arbitrary::arbitrary(g),
      Arbitrary::arbitrary(g),
    )
  }
}
