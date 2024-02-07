use std::{
  future::Future,
  time::{Duration, Instant},
};

use bytes::Bytes;
use futures::AsyncRead;
pub use nodecraft::{resolver::AddressResolver, *};

use crate::types::*;

use super::*;

pub mod stream;
use stream::*;

mod lpe;
pub use lpe::*;

/// Predefined unit tests for the transport module
#[cfg(any(test, feature = "test"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests;

/// `MaybeResolvedAddress` is used to represent an address that may or may not be resolved.
pub enum MaybeResolvedAddress<T: Transport> {
  /// The resolved address, which means that can be directly used to communicate with the remote node.
  Resolved(<T::Resolver as AddressResolver>::ResolvedAddress),
  /// The unresolved address, which means that need to be resolved before using it to communicate with the remote node.
  Unresolved(<T::Resolver as AddressResolver>::Address),
}

impl<T: Transport> Clone for MaybeResolvedAddress<T> {
  fn clone(&self) -> Self {
    match self {
      Self::Resolved(addr) => Self::Resolved(addr.clone()),
      Self::Unresolved(addr) => Self::Unresolved(addr.clone()),
    }
  }
}

impl<T: Transport> CheapClone for MaybeResolvedAddress<T> {
  fn cheap_clone(&self) -> Self {
    match self {
      Self::Resolved(addr) => Self::Resolved(addr.cheap_clone()),
      Self::Unresolved(addr) => Self::Unresolved(addr.cheap_clone()),
    }
  }
}

impl<T: Transport> core::fmt::Debug for MaybeResolvedAddress<T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Resolved(addr) => write!(f, "{addr:?}"),
      Self::Unresolved(addr) => write!(f, "{addr:?}"),
    }
  }
}

impl<T: Transport> core::fmt::Display for MaybeResolvedAddress<T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Resolved(addr) => write!(f, "{addr}"),
      Self::Unresolved(addr) => write!(f, "{addr}"),
    }
  }
}

impl<T: Transport> PartialEq for MaybeResolvedAddress<T> {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Resolved(addr1), Self::Resolved(addr2)) => addr1 == addr2,
      (Self::Unresolved(addr1), Self::Unresolved(addr2)) => addr1 == addr2,
      _ => false,
    }
  }
}

impl<T: Transport> Eq for MaybeResolvedAddress<T> {}

impl<T: Transport> core::hash::Hash for MaybeResolvedAddress<T> {
  fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
    match self {
      Self::Resolved(addr) => addr.hash(state),
      Self::Unresolved(addr) => addr.hash(state),
    }
  }
}

impl<T: Transport> MaybeResolvedAddress<T> {
  /// Creates a resolved address.
  #[inline(always)]
  pub const fn resolved(addr: <T::Resolver as AddressResolver>::ResolvedAddress) -> Self {
    Self::Resolved(addr)
  }

  /// Creates an unresolved address.
  #[inline(always)]
  pub const fn unresolved(addr: <T::Resolver as AddressResolver>::Address) -> Self {
    Self::Unresolved(addr)
  }

  /// Returns `true` if the address is resolved.
  #[inline(always)]
  pub fn is_resolved(&self) -> bool {
    matches!(self, Self::Resolved(_))
  }

  /// Returns `true` if the address is unresolved.
  #[inline(always)]
  pub fn is_unresolved(&self) -> bool {
    matches!(self, Self::Unresolved(_))
  }

  /// Returns the resolved address if it's resolved, otherwise returns `None`.
  #[inline(always)]
  pub fn as_resolved(&self) -> Option<&<T::Resolver as AddressResolver>::ResolvedAddress> {
    match self {
      Self::Resolved(addr) => Some(addr),
      Self::Unresolved(_) => None,
    }
  }

  /// Returns the unresolved address if it's unresolved, otherwise returns `None`.
  #[inline(always)]
  pub fn as_unresolved(&self) -> Option<&<T::Resolver as AddressResolver>::Address> {
    match self {
      Self::Resolved(_) => None,
      Self::Unresolved(addr) => Some(addr),
    }
  }

  /// Returns the resolved address if it's resolved, otherwise returns `None`.
  #[inline(always)]
  pub fn as_resolved_mut(
    &mut self,
  ) -> Option<&mut <T::Resolver as AddressResolver>::ResolvedAddress> {
    match self {
      Self::Resolved(addr) => Some(addr),
      Self::Unresolved(_) => None,
    }
  }

  /// Returns the unresolved address if it's unresolved, otherwise returns `None`.
  #[inline(always)]
  pub fn as_unresolved_mut(&mut self) -> Option<&mut <T::Resolver as AddressResolver>::Address> {
    match self {
      Self::Resolved(_) => None,
      Self::Unresolved(addr) => Some(addr),
    }
  }

  /// Returns the resolved address if it's resolved, otherwise returns `None`.
  #[inline(always)]
  pub fn into_resolved(self) -> Option<<T::Resolver as AddressResolver>::ResolvedAddress> {
    match self {
      Self::Resolved(addr) => Some(addr),
      Self::Unresolved(_) => None,
    }
  }

  /// Returns the unresolved address if it's unresolved, otherwise returns `None`.
  #[inline(always)]
  pub fn into_unresolved(self) -> Option<<T::Resolver as AddressResolver>::Address> {
    match self {
      Self::Resolved(_) => None,
      Self::Unresolved(addr) => Some(addr),
    }
  }
}

/// Ensures that the stream has timeout capabilities.
#[auto_impl::auto_impl(Box)]
pub trait TimeoutableReadStream: Unpin + Send + Sync + 'static {
  fn set_read_timeout(&mut self, timeout: Option<Duration>);

  fn read_timeout(&self) -> Option<Duration>;
}

/// Ensures that the stream has timeout capabilities.
#[auto_impl::auto_impl(Box)]
pub trait TimeoutableWriteStream: Unpin + Send + Sync + 'static {
  fn set_write_timeout(&mut self, timeout: Option<Duration>);

  fn write_timeout(&self) -> Option<Duration>;
}

/// Ensures that the stream has timeout capabilities.
pub trait TimeoutableStream:
  TimeoutableReadStream + TimeoutableWriteStream + Unpin + Send + Sync + 'static
{
  fn set_timeout(&mut self, timeout: Option<Duration>) {
    Self::set_read_timeout(self, timeout);
    Self::set_write_timeout(self, timeout);
  }

  fn timeout(&self) -> (Option<Duration>, Option<Duration>) {
    (Self::read_timeout(self), Self::write_timeout(self))
  }
}

impl<T: TimeoutableReadStream + TimeoutableWriteStream + Unpin + Send + Sync + 'static>
  TimeoutableStream for T
{
}

/// An error for the transport layer.
pub trait TransportError: std::error::Error + Send + Sync + 'static {
  /// Returns `true` if the error is a remote failure.
  ///
  /// e.g. Errors happened when:
  /// 1. Fail to send to a remote node
  /// 2. Fail to receive from a remote node.
  /// 3. Fail to dial a remote node.
  /// ...
  ///
  /// The above errors can be treated as remote failures.
  fn is_remote_failure(&self) -> bool;

  /// Custom the error.
  fn custom(err: std::borrow::Cow<'static, str>) -> Self;
}

/// The `Wire` trait for encoding and decoding of messages.
#[auto_impl::auto_impl(Box, Arc)]
pub trait Wire: Send + Sync + 'static {
  /// The error type for encoding and decoding
  type Error: std::error::Error + Send + Sync + 'static;

  /// The id type used to identify nodes
  type Id: Transformable;

  /// The resolved address type used to identify nodes
  type Address: Transformable;

  /// Returns the encoded length of the given message
  fn encoded_len(msg: &Message<Self::Id, Self::Address>) -> usize;

  /// Encodes the given message into the given buffer, returns the number of bytes written
  fn encode_message(
    msg: Message<Self::Id, Self::Address>,
    dst: &mut [u8],
  ) -> Result<usize, Self::Error>;

  /// Encodes the given message into the vec.
  fn encode_message_to_vec(msg: Message<Self::Id, Self::Address>) -> Result<Vec<u8>, Self::Error> {
    let mut buf = vec![0; Self::encoded_len(&msg)];
    Self::encode_message(msg, &mut buf)?;
    Ok(buf)
  }

  /// Encodes the given message into the bytes.
  fn encode_message_to_bytes(msg: Message<Self::Id, Self::Address>) -> Result<Bytes, Self::Error> {
    Self::encode_message_to_vec(msg).map(Into::into)
  }

  /// Decodes the given bytes into a message, returning how many bytes were read
  fn decode_message(src: &[u8]) -> Result<(usize, Message<Self::Id, Self::Address>), Self::Error>;

  /// Decode message from the reader and returns the number of bytes read and the message.
  fn decode_message_from_reader(
    conn: impl AsyncRead + Send + Unpin,
  ) -> impl Future<Output = std::io::Result<(usize, Message<Self::Id, Self::Address>)>> + Send;
}

/// Transport is used to abstract over communicating with other peers. The packet
/// interface is assumed to be best-effort and the stream interface is assumed to
/// be reliable.
#[auto_impl::auto_impl(Box, Arc)]
pub trait Transport: Sized + Send + Sync + 'static {
  /// The error type for the transport
  type Error: TransportError;
  /// The id type used to identify nodes
  type Id: Id;
  /// The address resolver used to resolve addresses
  type Resolver: AddressResolver<Runtime = Self::Runtime>;
  /// The promised stream used to send and receive messages
  type Stream: TimeoutableStream + Send + Sync + 'static;
  /// The wire used to encode and decode messages
  type Wire: Wire<Id = Self::Id, Address = <Self::Resolver as AddressResolver>::ResolvedAddress>;
  /// The async runtime
  type Runtime: agnostic::Runtime;

  /// Resolves the given address to a resolved address
  fn resolve(
    &self,
    addr: &<Self::Resolver as AddressResolver>::Address,
  ) -> impl Future<Output = Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error>>
       + Send;

  /// Returns the local id of the node
  fn local_id(&self) -> &Self::Id;

  /// Returns the local address of the node
  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address;

  /// Returns the advertise address of the node
  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress;

  /// Returns the maximum size of a packet that can be sent
  fn max_payload_size(&self) -> usize;

  /// Returns the size of header overhead when trying to send messages through packet stream ([`send_packets`]).
  ///
  /// e.g. if every time invoking [`send_packets`],
  /// the concrete implementation wants to  add a header of 10 bytes,
  /// then the packet overhead is 10 bytes.
  ///
  /// [`send_packets`]: #method.send_packets
  fn packets_header_overhead(&self) -> usize;

  /// Returns the size of overhead for per [`Message`] when trying to send messages through packet stream ([`send_packets`]).
  fn packet_overhead(&self) -> usize;

  /// Returns an error if the given address is blocked
  fn blocked_address(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error>;

  /// Reads a message from the remote node by promised connection.
  ///
  /// Returns the number of bytes read and the message.
  fn read_message(
    &self,
    from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> impl Future<
    Output = Result<
      (
        usize,
        Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
      ),
      Self::Error,
    >,
  > + Send;

  /// Sends a message to the remote node by promised connection.
  ///
  /// Returns the number of bytes sent.
  fn send_message(
    &self,
    conn: &mut Self::Stream,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// A packet-oriented interface that fires off the given
  /// payload to the given address in a connectionless fashion.
  ///
  /// # Returns
  ///
  /// - number of bytes sent
  /// - a time stamp that's as close as possible to when the packet
  /// was transmitted to help make accurate RTT measurements during probes.
  fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> impl Future<Output = Result<(usize, Instant), Self::Error>> + Send;

  /// A packet-oriented interface that fires off the given
  /// payload to the given address in a connectionless fashion.
  ///
  /// # Returns
  ///
  /// - number of bytes sent
  /// - a time stamp that's as close as possible to when the packet
  /// was transmitted to help make accurate RTT measurements during probes.
  fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> impl Future<Output = Result<(usize, Instant), Self::Error>> + Send;

  /// Used to create a connection that allows us to perform
  /// two-way communication with a peer. This is generally more expensive
  /// than packet connections so is used for more infrequent operations
  /// such as anti-entropy or fallback probes if the packet-oriented probe
  /// failed.
  fn dial_timeout(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    timeout: Duration,
  ) -> impl Future<Output = Result<Self::Stream, Self::Error>> + Send;

  /// Used to cache a connection for future use.
  fn cache_stream(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    stream: Self::Stream,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns a packet subscriber that can be used to receive incoming packets
  fn packet(
    &self,
  ) -> PacketSubscriber<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>;

  /// Returns a receiver that can be read to handle incoming stream
  /// connections from other peers. How this is set up for listening is
  /// left as an exercise for the concrete transport implementations.
  fn stream(
    &self,
  ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Stream>;

  /// Shutdown the transport
  fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
