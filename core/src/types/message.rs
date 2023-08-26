use super::*;

#[derive(Clone)]
#[repr(transparent)]
pub struct Message(pub(crate) BytesMut);

impl core::fmt::Debug for Message {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", self.0.as_ref())
  }
}

impl PartialEq for Message {
  fn eq(&self, other: &Self) -> bool {
    self.underlying_bytes() == other.underlying_bytes()
  }
}

impl Eq for Message {}

impl core::hash::Hash for Message {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.underlying_bytes().hash(state)
  }
}

impl Default for Message {
  fn default() -> Self {
    Self::new()
  }
}

macro_rules! put {
  ($($ty: ident), +$(,)?) => {
    $(
      paste::paste! {
        pub fn [<put _ $ty>](&mut self, val: $ty) {
          self.0.[<put _ $ty>](val);
        }

        pub fn [<put _ $ty _ le>](&mut self, val: $ty) {
          self.0.[<put _ $ty _ le>](val);
        }

        pub fn [<put _ $ty _ ne>](&mut self, val: $ty) {
          self.0.[<put _ $ty _ ne>](val);
        }
      }
    )*
  };
}

impl Message {
  const PREFIX_SIZE: usize = 1;

  #[inline]
  pub fn new() -> Self {
    let mut bytes = BytesMut::with_capacity(Self::PREFIX_SIZE);
    bytes.put_u8(MessageType::User as u8);
    Self(bytes)
  }

  #[doc(hidden)]
  #[inline]
  pub fn __from_bytes_mut(data: BytesMut) -> Self {
    Self(data)
  }

  pub(crate) fn compounds(mut msgs: Vec<Message>) -> Vec<Message> {
    const MAX_MESSAGES: usize = 255;

    let mut bufs = Vec::with_capacity((msgs.len() + MAX_MESSAGES - 1) / MAX_MESSAGES);

    while msgs.len() > MAX_MESSAGES {
      bufs.push(Self::compound(msgs.drain(..MAX_MESSAGES).collect()));
    }

    if !msgs.is_empty() {
      bufs.push(Self::compound(msgs));
    }

    bufs
  }

  pub(crate) fn compound(msgs: Vec<Message>) -> Message {
    let num_msgs = msgs.len();
    let total: usize = msgs.iter().map(|m| m.len()).sum();
    let msg_len = core::mem::size_of::<u8>() + num_msgs * core::mem::size_of::<u16>() + total;
    let mut buf = BytesMut::with_capacity(ENCODE_HEADER_SIZE + msg_len);
    // Write out the type
    let msg_len = (msg_len as u32).to_be_bytes();
    buf.put_slice(&[
      MessageType::Compound as u8,
      0,
      0,
      0,
      msg_len[0],
      msg_len[1],
      msg_len[2],
      msg_len[3],
    ]);
    // Write out the number of message
    buf.put_u8(num_msgs as u8);

    let mut compound = buf.split_off(num_msgs * 2);
    for msg in msgs {
      // Add the message length
      buf.put_u16(msg.len() as u16);
      // put msg into compound
      compound.put_slice(&msg);
    }

    buf.unsplit(compound);
    Message(buf)
  }

  #[inline]
  pub fn with_capacity(cap: usize) -> Self {
    let mut bytes = BytesMut::with_capacity(Self::PREFIX_SIZE + cap);
    bytes.put_u8(MessageType::User as u8);
    Self(bytes)
  }

  #[inline]
  pub fn resize(&mut self, new_len: usize, val: u8) {
    self.0.resize(new_len + Self::PREFIX_SIZE, val);
  }

  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.0.reserve(additional);
  }

  #[inline]
  pub fn truncate(&mut self, len: usize) {
    self.0.truncate(Self::PREFIX_SIZE + len)
  }

  #[inline]
  pub fn put_slice(&mut self, buf: &[u8]) {
    self.0.put_slice(buf)
  }

  #[inline]
  pub fn put_bool(&mut self, val: bool) {
    self.0.put_u8(val as u8);
  }

  #[inline]
  pub fn put_u8(&mut self, val: u8) {
    self.0.put_u8(val);
  }

  #[inline]
  pub fn put_i8(&mut self, val: i8) {
    self.0.put_i8(val);
  }

  put!(i16, i32, i64, i128, u16, u32, u64, u128, f32, f64);

  #[inline]
  pub fn put_bytes(&mut self, val: u8, cnt: usize) {
    self.0.put_bytes(val, cnt)
  }

  #[inline]
  pub fn clear(&mut self) {
    let mt = self.0[0];
    self.0.clear();
    self.0.put_u8(mt);
  }

  #[inline]
  pub fn as_slice(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub fn as_slice_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }

  #[inline]
  pub(crate) fn freeze(self) -> Bytes {
    self.0.freeze()
  }

  #[inline]
  pub(crate) fn underlying_bytes(&self) -> &[u8] {
    &self.0
  }

  #[inline]
  pub(crate) fn write_message_len(&mut self) {
    let len = (self.0.len() - ENCODE_HEADER_SIZE) as u32;
    self.0[ENCODE_META_SIZE..ENCODE_HEADER_SIZE].copy_from_slice(&len.to_be_bytes());
  }
}

impl std::io::Write for Message {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.0.put_slice(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
  }
}

impl Deref for Message {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.0[Self::PREFIX_SIZE..]
  }
}

impl DerefMut for Message {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0[Self::PREFIX_SIZE..]
  }
}

impl AsRef<[u8]> for Message {
  fn as_ref(&self) -> &[u8] {
    &self.0[Self::PREFIX_SIZE..]
  }
}

impl AsMut<[u8]> for Message {
  fn as_mut(&mut self) -> &mut [u8] {
    &mut self.0[Self::PREFIX_SIZE..]
  }
}

/// An ID of a type of message that can be received
/// on network channels from other members.
///
/// The list of available message types.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[repr(u8)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(u8), non_exhaustive)]
#[non_exhaustive]
pub(crate) enum MessageType {
  Ping = 0,
  IndirectPing = 1,
  AckResponse = 2,
  Suspect = 3,
  Alive = 4,
  Dead = 5,
  PushPull = 6,
  Compound = 7,
  /// User mesg, not handled by us
  User = 8,
  Compress = 9,
  Encrypt = 10,
  NackResponse = 11,
  HasCrc = 12,
  ErrorResponse = 13,
  /// HasLabel has a deliberately high value so that you can disambiguate
  /// it from the encryptionVersion header which is either 0/1 right now and
  /// also any of the existing [`MessageType`].
  HasLabel = 244,
}

impl core::fmt::Display for MessageType {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl MessageType {
  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();

  /// Returns the str of the [`MessageType`].
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      MessageType::Ping => "ping",
      MessageType::IndirectPing => "indirect ping",
      MessageType::AckResponse => "ack response",
      MessageType::Suspect => "suspect",
      MessageType::Alive => "alive",
      MessageType::Dead => "dead",
      MessageType::PushPull => "push pull",
      MessageType::Compound => "compound",
      MessageType::User => "user",
      MessageType::Compress => "compress",
      MessageType::Encrypt => "encrypt",
      MessageType::NackResponse => "nack response",
      MessageType::HasCrc => "crc",
      MessageType::ErrorResponse => "error",
      MessageType::HasLabel => "label",
    }
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidMessageType(u8);

impl core::fmt::Display for InvalidMessageType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid message type: {}", self.0)
  }
}

impl std::error::Error for InvalidMessageType {}

impl TryFrom<u8> for MessageType {
  type Error = InvalidMessageType;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0 => Ok(Self::Ping),
      1 => Ok(Self::IndirectPing),
      2 => Ok(Self::AckResponse),
      3 => Ok(Self::Suspect),
      4 => Ok(Self::Alive),
      5 => Ok(Self::Dead),
      6 => Ok(Self::PushPull),
      7 => Ok(Self::Compound),
      8 => Ok(Self::User),
      9 => Ok(Self::Compress),
      10 => Ok(Self::Encrypt),
      11 => Ok(Self::NackResponse),
      12 => Ok(Self::HasCrc),
      13 => Ok(Self::ErrorResponse),
      244 => Ok(Self::HasLabel),
      _ => Err(InvalidMessageType(value)),
    }
  }
}

pub(crate) type CompositeSerializer<W, const N: usize> =
  rkyv::ser::serializers::CompositeSerializer<
    WriteSerializer<W>,
    FallbackScratch<HeapScratch<N>, AllocScratch>,
    SharedSerializeMap,
  >;

pub(crate) struct MessageSerializer<
  const N: usize = DEFAULT_ENCODE_PREALLOCATE_SIZE,
  W: std::io::Write = Message,
>(CompositeSerializer<W, N>);

impl Default for MessageSerializer {
  fn default() -> Self {
    Self::new()
  }
}

impl MessageSerializer {
  pub(crate) fn new() -> Self {
    Self::with_writter(Message(BytesMut::with_capacity(
      DEFAULT_ENCODE_PREALLOCATE_SIZE - 1,
    )))
  }
}

impl<const N: usize> MessageSerializer<N> {
  pub(crate) fn with_preallocated_size() -> Self {
    Self::with_writter(Message(BytesMut::with_capacity(N - 1)))
  }
}

impl<W: std::io::Write, const N: usize> MessageSerializer<N, W> {
  pub(crate) fn with_writter(w: W) -> Self {
    Self(CompositeSerializer::new(
      WriteSerializer::new(w),
      Default::default(),
      Default::default(),
    ))
  }

  pub(crate) fn into_writter(self) -> W {
    self.0.into_serializer().into_inner()
  }
}

impl<W: std::io::Write, const N: usize> core::ops::Deref for MessageSerializer<N, W> {
  type Target = CompositeSerializer<W, N>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<W: std::io::Write, const N: usize> core::ops::DerefMut for MessageSerializer<N, W> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}
