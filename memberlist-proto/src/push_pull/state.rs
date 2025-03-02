use nodecraft::{CheapClone, Node};

use crate::{
  Data, DataRef, DecodeError, DelegateVersion, EncodeError, Meta, ProtocolVersion, State, WireType,
  merge, skip, split,
};

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
      match src[offset] {
        b if b == PushNodeState::<I, A>::id_byte() => {
          if id.is_some() {
            return Err(DecodeError::duplicate_field("PushNodeState", "id", ID_TAG));
          }
          offset += 1;

          let (readed, value) = I::Ref::decode_length_delimited(&src[offset..])?;
          offset += readed;
          id = Some(value);
        }
        b if b == PushNodeState::<I, A>::addr_byte() => {
          if addr.is_some() {
            return Err(DecodeError::duplicate_field(
              "PushNodeState",
              "addr",
              ADDR_TAG,
            ));
          }
          offset += 1;

          let (readed, value) = A::Ref::decode_length_delimited(&src[offset..])?;
          offset += readed;
          addr = Some(value);
        }
        META_BYTE => {
          if meta.is_some() {
            return Err(DecodeError::duplicate_field(
              "PushNodeState",
              "meta",
              META_TAG,
            ));
          }
          offset += 1;

          let (readed, value) = <&[u8] as DataRef<Meta>>::decode_length_delimited(&src[offset..])?;
          offset += readed;
          meta = Some(value);
        }
        INCARNATION_BYTE => {
          if incarnation.is_some() {
            return Err(DecodeError::duplicate_field(
              "PushNodeState",
              "incarnation",
              INCARNATION_TAG,
            ));
          }
          offset += 1;

          let (readed, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
          offset += readed;
          incarnation = Some(value);
        }
        STATE_BYTE => {
          if state.is_some() {
            return Err(DecodeError::duplicate_field(
              "PushNodeState",
              "state",
              STATE_TAG,
            ));
          }
          offset += 1;

          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let value = State::from(src[offset]);
          offset += 1;
          state = Some(value);
        }
        PROTOCOL_VERSION_BYTE => {
          if protocol_version.is_some() {
            return Err(DecodeError::duplicate_field(
              "PushNodeState",
              "protocol_version",
              PROTOCOL_VERSION_TAG,
            ));
          }
          offset += 1;

          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let value = ProtocolVersion::from(src[offset]);
          offset += 1;
          protocol_version = Some(value);
        }
        DELEGATE_VERSION_BYTE => {
          if delegate_version.is_some() {
            return Err(DecodeError::duplicate_field(
              "PushNodeState",
              "delegate_version",
              DELEGATE_VERSION_TAG,
            ));
          }
          offset += 1;

          if offset >= src.len() {
            return Err(DecodeError::buffer_underflow());
          }
          let value = DelegateVersion::from(src[offset]);
          offset += 1;
          delegate_version = Some(value);
        }
        b => {
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
    let meta_len = self.meta.len();
    if meta_len != 0 {
      len += 1 + self.meta.encoded_len_with_length_delimited();
    }
    len += 1 + self.incarnation.encoded_len();
    len += 1 + 1; // state
    len += 1 + 1; // protocol version
    len += 1 + 1; // delegate version
    len
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(EncodeError::insufficient_buffer(self.encoded_len(), $len));
        }
      };
    }

    let mut offset = 0;
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

    let meta_len = self.meta.len();
    if meta_len != 0 {
      bail!(self(offset, len));
      buf[offset] = META_BYTE;
      offset += 1;
      offset += self
        .meta
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(self.encoded_len(), len))?;
    }

    bail!(self(offset, len));
    buf[offset] = INCARNATION_BYTE;
    offset += 1;
    offset += self
      .incarnation
      .encode(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), len))?;

    bail!(self(offset, len));
    buf[offset] = STATE_BYTE;
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = self.state.into();
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = PROTOCOL_VERSION_BYTE;
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = self.protocol_version.into();
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = DELEGATE_VERSION_BYTE;
    offset += 1;

    bail!(self(offset, len));
    buf[offset] = self.delegate_version.into();
    offset += 1;

    #[cfg(debug_assertions)]
    crate::debug_assert_write_eq::<Self>(offset, self.encoded_len());
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
