use core::marker::PhantomData;

use nodecraft::{CheapClone, Node};

use crate::{
  Data, DataRef, DecodeError, DelegateVersion, EncodeError, Meta, ProtocolVersion, State, WireType,
  merge, skip, split,
};

use super::STATES_BYTE;

/// Push node state is the state push to the remote server.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

const ID_TAG: u8 = 1;
const ADDR_TAG: u8 = 2;
const META_TAG: u8 = 3;
const META_BYTE: u8 = merge(WireType::LengthDelimited, META_TAG);
const INCARNATION_TAG: u8 = 4;
const INCARNATION_BYTE: u8 = merge(WireType::Varint, INCARNATION_TAG);
const STATE_TAG: u8 = 5;
const STATE_BYTE: u8 = merge(WireType::Byte, STATE_TAG);
const PROTOCOL_VERSION_TAG: u8 = 6;
const PROTOCOL_VERSION_BYTE: u8 = merge(WireType::Byte, PROTOCOL_VERSION_TAG);
const DELEGATE_VERSION_TAG: u8 = 7;
const DELEGATE_VERSION_BYTE: u8 = merge(WireType::Byte, DELEGATE_VERSION_TAG);

impl<I, A> PushNodeState<I, A>
where
  I: Data,
  A: Data,
{
  const fn id_byte() -> u8 {
    merge(I::WIRE_TYPE, ID_TAG)
  }

  const fn addr_byte() -> u8 {
    merge(A::WIRE_TYPE, ADDR_TAG)
  }
}

/// Push node state is the state push to the remote server.
#[viewit::viewit(getters(vis_all = "pub"), setters(skip))]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct PushNodeStateRef<'a, I, A> {
  /// The id of the push node state.
  #[viewit(getter(
    const,
    style = "ref",
    attrs(doc = "Returns the id of the push node state")
  ))]
  id: I,
  /// The address of the push node state.
  #[viewit(getter(
    const,
    rename = "address",
    style = "ref",
    attrs(doc = "Returns the address of the push node state")
  ))]
  addr: A,
  /// Metadata from the delegate for this push node state.
  #[viewit(getter(
    const,
    style = "ref",
    attrs(doc = "Returns the meta of the push node state")
  ))]
  meta: &'a [u8],
  /// The incarnation of the push node state.
  #[viewit(getter(const, attrs(doc = "Returns the incarnation of the push node state")))]
  incarnation: u32,
  /// The state of the push node state.
  #[viewit(getter(const, attrs(doc = "Returns the state of the push node state")))]
  state: State,
  /// The protocol version of the push node state is speaking.
  #[viewit(getter(
    const,
    attrs(doc = "Returns the protocol version of the push node state is speaking")
  ))]
  protocol_version: ProtocolVersion,
  /// The delegate version of the push node state is speaking.
  #[viewit(getter(
    const,
    attrs(doc = "Returns the delegate version of the push node state is speaking")
  ))]
  delegate_version: DelegateVersion,
}

impl<'a, I, A> DataRef<'a, PushNodeState<I, A>> for PushNodeStateRef<'a, I::Ref<'a>, A::Ref<'a>>
where
  I: Data,
  A: Data,
{
  fn decode(src: &'a [u8]) -> Result<(usize, Self), crate::DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let mut id = None;
    let mut addr = None;
    let mut meta = None;
    let mut incarnation = None;
    let mut state = None;
    let mut protocol_version = None;
    let mut delegate_version = None;

    while offset < src.len() {
      let b = src[offset];
      offset += 1;

      match b {
        b if b == PushNodeState::<I, A>::id_byte() => {
          let (readed, value) = I::Ref::decode_length_delimited(&src[offset..])?;
          offset += readed;
          id = Some(value);
        }
        b if b == PushNodeState::<I, A>::addr_byte() => {
          let (readed, value) = A::Ref::decode_length_delimited(&src[offset..])?;
          offset += readed;
          addr = Some(value);
        }
        META_BYTE => {
          let (readed, value) = <&[u8] as DataRef<Meta>>::decode_length_delimited(&src[offset..])?;
          offset += readed;
          meta = Some(value);
        }
        INCARNATION_BYTE => {
          let (readed, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
          offset += readed;
          incarnation = Some(value);
        }
        STATE_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let value = State::from(src[offset]);
          offset += 1;
          state = Some(value);
        }
        PROTOCOL_VERSION_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let value = ProtocolVersion::from(src[offset]);
          offset += 1;
          protocol_version = Some(value);
        }
        DELEGATE_VERSION_BYTE => {
          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let value = DelegateVersion::from(src[offset]);
          offset += 1;
          delegate_version = Some(value);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
          offset += skip(wire_type, &src[offset..])?;
        }
      }
    }

    Ok((
      offset,
      Self {
        id: id.ok_or_else(|| DecodeError::missing_field("PushNodeState", "id"))?,
        addr: addr.ok_or_else(|| DecodeError::missing_field("PushNodeState", "missing addr"))?,
        meta: meta.unwrap_or_default(),
        incarnation: incarnation
          .ok_or_else(|| DecodeError::missing_field("PushNodeState", "incarnation"))?,
        state: state.ok_or_else(|| DecodeError::missing_field("PushNodeState", "state"))?,
        protocol_version: protocol_version.unwrap_or_default(),
        delegate_version: delegate_version.unwrap_or_default(),
      },
    ))
  }
}

impl<I, A> Data for PushNodeState<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = PushNodeStateRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    I::from_ref(val.id)
      .and_then(|id| A::from_ref(val.addr).map(|addr| (id, addr)))
      .and_then(|(id, addr)| Meta::from_ref(val.meta).map(|meta| (meta, id, addr)))
      .map(|(meta, id, addr)| Self {
        id,
        addr,
        meta,
        incarnation: val.incarnation,
        state: val.state,
        protocol_version: val.protocol_version,
        delegate_version: val.delegate_version,
      })
  }

  fn encoded_len(&self) -> usize {
    let mut len = 1 + self.id.encoded_len_with_length_delimited();
    len += 1 + self.addr.encoded_len_with_length_delimited();
    len += 1 + self.meta.encoded_len_with_length_delimited();
    len += 1 + self.incarnation.encoded_len();
    len += 1 + 1; // state
    len += 1 + 1; // protocol version
    len += 1 + 1; // delegate version
    len
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, crate::EncodeError> {
    let mut offset = 0;
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(EncodeError::insufficient_buffer($offset, $len));
        }
      };
    }

    let len = buf.len();
    bail!(self(0, len));
    buf[offset] = Self::id_byte();
    offset += 1;
    offset += self
      .id
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = Self::addr_byte();
    offset += 1;
    offset += self
      .addr
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = META_BYTE;
    offset += 1;
    offset += self
      .meta
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = INCARNATION_BYTE;
    offset += 1;
    offset += self
      .incarnation
      .encode(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset + 6, len));
    buf[offset] = STATE_BYTE;
    offset += 1;
    buf[offset] = self.state.into();
    offset += 1;
    buf[offset] = PROTOCOL_VERSION_BYTE;
    offset += 1;
    buf[offset] = self.protocol_version.into();
    offset += 1;
    buf[offset] = DELEGATE_VERSION_BYTE;
    offset += 1;
    buf[offset] = self.delegate_version.into();
    offset += 1;

    #[cfg(debug_assertions)]
    crate::debug_assert_write_eq(offset, self.encoded_len());
    Ok(offset)
  }
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

/// A reference type to a collection of [`PushNodeState`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PushNodeStatesDecoder<'a> {
  src: &'a [u8],
  start_offset: usize,
  end_offset: usize,
  num_states: usize,
}

impl<'a> PushNodeStatesDecoder<'a> {
  pub(super) fn new(src: &'a [u8], num_states: usize, offsets: Option<(usize, usize)>) -> Self {
    let (start, end) = offsets.map_or((0, 0), |(start, end)| (start, end));
    Self {
      src,
      start_offset: start,
      end_offset: end,
      num_states,
    }
  }

  /// Returns the number of [`PushNodeState`] in the collection.
  #[inline]
  pub const fn len(&self) -> usize {
    self.num_states
  }

  /// Returns `true` if the collection is empty.
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.num_states == 0
  }

  /// Returns an iterator over the [`PushNodeState`] in the collection.
  pub fn iter<I, A>(&self) -> PushNodeStatesDecodeIter<'a, I, A>
  where
    I: Data,
    A: Data,
  {
    PushNodeStatesDecodeIter {
      src: self.src,
      current_offset: (self.num_states > 0).then_some(self.start_offset),
      end_offset: self.end_offset,
      num_states: self.num_states,
      _phantom: PhantomData,
    }
  }
}

/// An iterator over the [`PushNodeStateRef`] in the collection.
pub struct PushNodeStatesDecodeIter<'a, I, A> {
  src: &'a [u8],
  current_offset: Option<usize>,
  end_offset: usize,
  num_states: usize,
  _phantom: PhantomData<(I, A)>,
}

impl<'a, I, A> Iterator for PushNodeStatesDecodeIter<'a, I, A>
where
  I: Data,
  A: Data,
{
  type Item = Result<PushNodeStateRef<'a, I::Ref<'a>, A::Ref<'a>>, DecodeError>;

  fn next(&mut self) -> Option<Self::Item> {
    let current_offset = self.current_offset.as_mut()?;

    while *current_offset < self.end_offset {
      // Parse the tag and wire type
      let b = self.src[*current_offset];
      *current_offset += 1;

      match b {
        STATES_BYTE => {
          let (readed, value) = match <PushNodeStateRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
            PushNodeState<I, A>,
          >>::decode_length_delimited(
            &self.src[*current_offset..]
          ) {
            Ok((readed, value)) => (readed, value),
            Err(e) => return Some(Err(e)),
          };
          *current_offset += readed;
          return Some(Ok(value));
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = match WireType::try_from(wire_type) {
            Ok(wire_type) => wire_type,
            Err(wt) => return Some(Err(DecodeError::unknown_wire_type(wt))),
          };
          *current_offset += match skip(wire_type, &self.src[*current_offset..]) {
            Ok(offset) => offset,
            Err(e) => return Some(Err(e)),
          };
        }
      }
    }
    None
  }
}

impl<I: Data, A: Data> core::iter::ExactSizeIterator for PushNodeStatesDecodeIter<'_, I, A> {
  fn len(&self) -> usize {
    self.num_states
  }
}

impl<I: Data, A: Data> core::iter::FusedIterator for PushNodeStatesDecodeIter<'_, I, A> {}
