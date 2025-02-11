use core::marker::PhantomData;

use bytes::Bytes;
use nodecraft::CheapClone;
use triomphe::Arc;

use super::{merge, skip, split, Data, DataRef, DecodeError, EncodeError, WireType};

mod state;
pub use state::*;

/// Push pull message.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
  #[cfg_attr(feature = "arbitrary", arbitrary(with = super::arbitrary_triomphe_arc))]
  states: Arc<[PushNodeState<I, A>]>,
  /// The user data of the push pull message.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the user data of the push pull message")
    ),
    setter(attrs(doc = "Sets the user data of the push pull message (Builder pattern)"))
  )]
  #[cfg_attr(feature = "arbitrary", arbitrary(with = super::arbitrary_bytes))]
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
      states: self.states.clone(),
      user_data: self.user_data.clone(),
    }
  }
}

const JOIN_TAG: u8 = 1;
const JOIN_BYTE: u8 = merge(WireType::Varint, JOIN_TAG);
const STATES_TAG: u8 = 2;
const STATES_BYTE: u8 = merge(WireType::LengthDelimited, STATES_TAG);
const USER_DATA_TAG: u8 = 3;
const USER_DATA_BYTE: u8 = merge(WireType::LengthDelimited, USER_DATA_TAG);

impl<I, A> PushPull<I, A> {
  /// Create a new [`PushPull`] message.
  #[inline]
  pub fn new(join: bool, states: impl Iterator<Item = PushNodeState<I, A>>) -> Self {
    Self {
      states: Arc::from_iter(states),
      user_data: Bytes::new(),
      join,
    }
  }

  /// Consumes the [`PushPull`] and returns the states and user data.
  #[inline]
  pub fn into_components(self) -> (bool, Bytes, Arc<[PushNodeState<I, A>]>) {
    (self.join, self.user_data, self.states)
  }
}

impl<I, A> Data for PushPull<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = PushPullRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    val
      .states
      .iter::<I, A>()
      .map(|res| res.and_then(PushNodeState::from_ref))
      .collect::<Result<Arc<[_]>, DecodeError>>()
      .map(|states| Self {
        join: val.join,
        states,
        user_data: Bytes::copy_from_slice(val.user_data),
      })
  }

  fn encoded_len(&self) -> usize {
    let mut len = 1 + 1; // join
    for i in self.states.iter() {
      len += 1 + i.encoded_len_with_length_delimited();
    }
    len += 1 + self.user_data.encoded_len_with_length_delimited();
    len
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut offset = 0;
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(EncodeError::insufficient_buffer($offset, $len).into());
        }
      };
    }

    let len = buf.len();
    bail!(self(1, len));
    buf[offset] = JOIN_BYTE;
    offset += 1;
    buf[offset] = self.join as u8;
    offset += 1;

    for i in self.states.iter() {
      bail!(self(offset, len));
      buf[offset] = STATES_BYTE;
      offset += 1;
      {
        offset += i
          .encode_length_delimited(&mut buf[offset..])
          .map_err(|e| e.update(self.encoded_len(), len))?
      }
    }

    bail!(self(offset, len));
    buf[offset] = USER_DATA_BYTE;
    offset += 1;
    offset += self.user_data.encode_length_delimited(&mut buf[offset..])?;

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq(offset, self.encoded_len());
    Ok(offset)
  }
}

/// The reference type of Push pull message.
#[viewit::viewit(getters(vis_all = "pub"), setters(skip))]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PushPullRef<'a, I, A> {
  /// Whether the push pull message is a join message.
  #[viewit(getter(
    const,
    attrs(doc = "Returns whether the push pull message is a join message")
  ))]
  join: bool,
  /// The states of the push pull message.
  #[viewit(getter(
    const,
    style = "ref",
    attrs(doc = "Returns the states of the push pull message")
  ))]
  states: PushNodeStatesDecoder<'a>,
  /// The user data of the push pull message.
  #[viewit(getter(const, attrs(doc = "Returns the user data of the push pull message")))]
  user_data: &'a [u8],

  #[viewit(getter(skip))]
  _m: PhantomData<(I, A)>,
}

impl<I, A> Clone for PushPullRef<'_, I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A> Copy for PushPullRef<'_, I, A> {}

impl<'a, I, A> DataRef<'a, PushPull<I, A>> for PushPullRef<'a, I::Ref<'a>, A::Ref<'a>>
where
  I: Data,
  A: Data,
{
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    let mut offset = 0;
    let mut join = None;
    let mut node_state_offsets = None;
    let mut num_states = 0;
    let mut user_data = None;

    while offset < src.len() {
      // Parse the tag and wire type
      let b = src[offset];
      offset += 1;

      match b {
        JOIN_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let val = src[offset];
          offset += 1;
          join = Some(val);
        }
        STATES_BYTE => {
          let readed = super::skip(WireType::LengthDelimited, &src[offset..])?;
          if let Some((ref mut fnso, ref mut lnso)) = node_state_offsets {
            if *fnso > offset {
              *fnso = offset - 1;
            }

            if *lnso < offset + readed {
              *lnso = offset + readed;
            }
          } else {
            node_state_offsets = Some((offset - 1, offset + readed));
          }
          num_states += 1;
          offset += readed;
        }
        USER_DATA_BYTE => {
          let (readed, value) = <&[u8] as DataRef<Bytes>>::decode_length_delimited(&src[offset..])?;
          offset += readed;
          user_data = Some(value);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    let join = join.ok_or(DecodeError::missing_field("PushPull", "join"))? != 0;
    let user_data = user_data.unwrap_or_default();
    Ok((
      offset,
      Self {
        join,
        states: PushNodeStatesDecoder::new(src, num_states, node_state_offsets),
        user_data,
        _m: PhantomData,
      },
    ))
  }
}

#[cfg(feature = "quickcheck")]
const _: () = {
  use super::*;
  use quickcheck::{Arbitrary, Gen};

  impl<I, A> Arbitrary for PushPull<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      let join = bool::arbitrary(g);
      let states = Vec::<PushNodeState<I, A>>::arbitrary(g);
      let user_data = Vec::<u8>::arbitrary(g).into();
      Self {
        join,
        states: Arc::from(states),
        user_data,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use arbitrary::{Arbitrary, Unstructured};

  use crate::{DelegateVersion, Meta, ProtocolVersion, State};

  use super::*;

  #[test]
  fn test_push_pull_clone_and_cheap_clone() {
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);
    let mut data = Unstructured::new(&data);

    let push_pull = PushPull::<String, SocketAddr>::arbitrary(&mut data).unwrap();
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
    let mut data = vec![0; 1024];
    rand::fill(&mut data[..]);
    let mut data = Unstructured::new(&data);

    let mut state = PushNodeState::<String, SocketAddr>::arbitrary(&mut data).unwrap();
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
  }
}
