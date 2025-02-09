use core::future::Future;

use agnostic_lite::RuntimeLite;
use bytes::Bytes;
pub use nodecraft::{resolver::AddressResolver, CheapClone, *};

use crate::types::*;

use super::*;

mod stream;
pub use stream::*;

/// Predefined unit tests for the transport module
#[cfg(any(test, feature = "test"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests;

/// `MaybeResolvedAddress` is used to represent an address that may or may not be resolved.
pub enum MaybeResolvedAddress<T: Transport> {
  /// The resolved address, which means that can be directly used to communicate with the remote node.
  Resolved(T::ResolvedAddress),
  /// The unresolved address, which means that need to be resolved before using it to communicate with the remote node.
  Unresolved(T::Address),
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
  #[inline]
  pub const fn resolved(addr: <T::Resolver as AddressResolver>::ResolvedAddress) -> Self {
    Self::Resolved(addr)
  }

  /// Creates an unresolved address.
  #[inline]
  pub const fn unresolved(addr: <T::Resolver as AddressResolver>::Address) -> Self {
    Self::Unresolved(addr)
  }

  /// Returns `true` if the address is resolved.
  #[inline]
  pub fn is_resolved(&self) -> bool {
    matches!(self, Self::Resolved(_))
  }

  /// Returns `true` if the address is unresolved.
  #[inline]
  pub fn is_unresolved(&self) -> bool {
    matches!(self, Self::Unresolved(_))
  }

  /// Returns the resolved address if it's resolved, otherwise returns `None`.
  #[inline]
  pub fn as_resolved(&self) -> Option<&<T::Resolver as AddressResolver>::ResolvedAddress> {
    match self {
      Self::Resolved(addr) => Some(addr),
      Self::Unresolved(_) => None,
    }
  }

  /// Returns the unresolved address if it's unresolved, otherwise returns `None`.
  #[inline]
  pub fn as_unresolved(&self) -> Option<&<T::Resolver as AddressResolver>::Address> {
    match self {
      Self::Resolved(_) => None,
      Self::Unresolved(addr) => Some(addr),
    }
  }

  /// Returns the resolved address if it's resolved, otherwise returns `None`.
  #[inline]
  pub fn as_resolved_mut(
    &mut self,
  ) -> Option<&mut <T::Resolver as AddressResolver>::ResolvedAddress> {
    match self {
      Self::Resolved(addr) => Some(addr),
      Self::Unresolved(_) => None,
    }
  }

  /// Returns the unresolved address if it's unresolved, otherwise returns `None`.
  #[inline]
  pub fn as_unresolved_mut(&mut self) -> Option<&mut <T::Resolver as AddressResolver>::Address> {
    match self {
      Self::Resolved(_) => None,
      Self::Unresolved(addr) => Some(addr),
    }
  }

  /// Returns the resolved address if it's resolved, otherwise returns `None`.
  #[inline]
  pub fn into_resolved(self) -> Option<<T::Resolver as AddressResolver>::ResolvedAddress> {
    match self {
      Self::Resolved(addr) => Some(addr),
      Self::Unresolved(_) => None,
    }
  }

  /// Returns the unresolved address if it's unresolved, otherwise returns `None`.
  #[inline]
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
  /// The instant type used to represent the deadline.
  type Instant: agnostic_lite::time::Instant + Send + Sync + 'static;

  /// Set the read deadline.
  fn set_read_deadline(&mut self, deadline: Option<Self::Instant>);

  /// Returns the read deadline.
  fn read_deadline(&self) -> Option<Self::Instant>;
}

/// Ensures that the stream has timeout capabilities.
#[auto_impl::auto_impl(Box)]
pub trait TimeoutableWriteStream: Unpin + Send + Sync + 'static {
  /// The instant type used to represent the deadline.
  type Instant: agnostic_lite::time::Instant + Send + Sync + 'static;

  /// Set the write deadline.
  fn set_write_deadline(&mut self, deadline: Option<Self::Instant>);

  /// Returns the write deadline.
  fn write_deadline(&self) -> Option<Self::Instant>;
}

/// Ensures that the stream has timeout capabilities.
pub trait TimeoutableStream:
  TimeoutableReadStream<Instant = <Self as TimeoutableStream>::Instant>
  + TimeoutableWriteStream<Instant = <Self as TimeoutableStream>::Instant>
  + Unpin
  + Send
  + Sync
  + 'static
{
  /// The instant type used to represent the deadline.
  type Instant: agnostic_lite::time::Instant + Send + Sync + 'static;

  /// Set the deadline for both read and write.
  fn set_deadline(&mut self, deadline: Option<<Self as TimeoutableStream>::Instant>) {
    Self::set_read_deadline(self, deadline);
    Self::set_write_deadline(self, deadline);
  }

  /// Returns the read deadline and the write deadline.
  fn deadline(
    &self,
  ) -> (
    Option<<Self as TimeoutableStream>::Instant>,
    Option<<Self as TimeoutableStream>::Instant>,
  ) {
    (Self::read_deadline(self), Self::write_deadline(self))
  }
}

impl<T> TimeoutableStream for T
where
  T: TimeoutableReadStream
    + TimeoutableWriteStream<Instant = <T as TimeoutableReadStream>::Instant>
    + Unpin
    + Send
    + Sync
    + 'static,
{
  type Instant = <T as TimeoutableReadStream>::Instant;
}

/// An error for the transport layer.
pub trait TransportError: std::error::Error + Send + Sync + 'static {
  /// Returns `true` if the error is a remote failure.
  ///
  /// e.g. Errors happened when:
  /// 1. Fail to send to a remote node
  /// 2. Fail to receive from a remote node.
  /// 3. Fail to dial a remote node.
  ///
  /// The above errors can be treated as remote failures.
  fn is_remote_failure(&self) -> bool;

  /// Custom the error.
  fn custom(err: std::borrow::Cow<'static, str>) -> Self;
}

/// Transport is used to abstract over communicating with other peers. The packet
/// interface is assumed to be best-effort and the stream interface is assumed to
/// be reliable.
pub trait Transport: Sized + Send + Sync + 'static {
  /// The error type for the transport
  type Error: TransportError;
  /// The id type used to identify nodes
  type Id: Id + Data + Send + Sync + 'static;
  /// The address type of the node
  type Address: Address + Send + Sync + 'static;
  /// The resolved address type of the node
  type ResolvedAddress: Data
    + CheapClone
    + core::hash::Hash
    + Eq
    + Ord
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static;

  /// The address resolver used to resolve addresses
  type Resolver: AddressResolver<
    Runtime = Self::Runtime,
    Address = Self::Address,
    ResolvedAddress = Self::ResolvedAddress,
  >;

  /// The promised stream used to send and receive messages
  type Stream: TimeoutableStream<Instant = <Self::Runtime as RuntimeLite>::Instant>
    + Send
    + Sync
    + 'static;
  /// The async runtime
  type Runtime: RuntimeLite;
  /// The options used to construct the transport
  type Options: Send + Sync + 'static;

  /// Creates a new transport with the given options
  fn new(options: Self::Options) -> impl Future<Output = Result<Self, Self::Error>> + Send;

  /// Resolves the given address to a resolved address
  fn resolve(
    &self,
    addr: &<Self::Resolver as AddressResolver>::Address,
  ) -> impl Future<Output = Result<Self::ResolvedAddress, Self::Error>> + Send;

  /// Returns the local id of the node
  fn local_id(&self) -> &Self::Id;

  /// Returns the local address of the node
  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address;

  /// Returns the advertise address of the node
  fn advertise_address(&self) -> &Self::ResolvedAddress;

  /// Returns the keyring (only used for encryption) of the node
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  fn keyring(&self) -> Option<&keyring::Keyring>;

  /// Returns if this transport enables encryption
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  fn encryption_enabled(&self) -> bool;

  /// Returns the maximum size of a packet that can be sent
  fn max_payload_size(&self) -> usize;

  /// Returns the size of header overhead when trying to send messages through packet stream ([`Transport::send_packets`]).
  ///
  /// e.g. if every time invoking [`Transport::send_packets`],
  /// the concrete implementation wants to  add a header of 10 bytes,
  /// then the packet overhead is 10 bytes.
  fn packets_header_overhead(&self) -> usize;

  /// Returns the size of overhead for per [`Message`] when trying to send messages through packet stream ([`Transport::send_packets`]).
  fn packet_overhead(&self) -> usize;

  /// Returns an error if the given address is blocked
  fn blocked_address(&self, addr: &Self::ResolvedAddress) -> Result<(), Self::Error>;

  /// Reads a message from the remote node by promised connection.
  ///
  /// Returns the number of bytes read and the message.
  fn read_message(
    &self,
    from: &Self::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> impl Future<Output = Result<(usize, Bytes), Self::Error>> + Send;

  /// Sends a message to the remote node by promised connection.
  ///
  /// The payload is encoded by [`Message::encode`] method,
  /// and can be decoded back by [`Message::decode`] method.
  ///
  /// The payload is not compressed or encrypted, the transport
  /// layer is expected to handle that by themselves.
  ///
  /// Returns the number of bytes sent.
  fn send_message(
    &self,
    conn: &mut Self::Stream,
    msg: Bytes,
  ) -> impl Future<Output = Result<usize, Self::Error>> + Send;

  /// A packet-oriented interface that fires off the given
  /// payload to the given address in a connectionless fashion.
  ///
  /// The payload is encoded by [`Message::encode`] method,
  /// and can be decoded back by [`Message::decode`] method.
  ///
  /// The payload is not compressed or encrypted, the transport
  /// layer is expected to handle that by themselves.
  ///
  /// # Returns
  ///
  /// - number of bytes sent
  /// - a time stamp that's as close as possible to when the packet
  ///   was transmitted to help make accurate RTT measurements during probes.
  fn send_packet(
    &self,
    addr: &Self::ResolvedAddress,
    packet: Bytes,
  ) -> impl Future<Output = Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error>> + Send;

  /// A packet-oriented interface that fires off the given
  /// payload to the given address in a connectionless fashion.
  ///
  /// The payload is encoded by [`Messages::encode`] method,
  /// and can be decoded back by [`Messages::decode`] method.
  ///
  /// The payload is not compressed or encrypted, the transport
  /// layer is expected to handle that by themselves.
  ///
  /// # Returns
  ///
  /// - number of bytes sent
  /// - a time stamp that's as close as possible to when the packet
  ///   was transmitted to help make accurate RTT measurements during probes.
  fn send_packets(
    &self,
    addr: &Self::ResolvedAddress,
    packet: Bytes,
  ) -> impl Future<Output = Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error>> + Send;

  /// Used to create a connection that allows us to perform
  /// two-way communication with a peer. This is generally more expensive
  /// than packet connections so is used for more infrequent operations
  /// such as anti-entropy or fallback probes if the packet-oriented probe
  /// failed.
  fn dial_with_deadline(
    &self,
    addr: &Self::ResolvedAddress,
    deadline: <Self::Runtime as RuntimeLite>::Instant,
  ) -> impl Future<Output = Result<Self::Stream, Self::Error>> + Send;

  /// Used to cache a connection for future use.
  fn cache_stream(
    &self,
    addr: &Self::ResolvedAddress,
    stream: Self::Stream,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;

  /// Returns a packet subscriber that can be used to receive incoming packets
  fn packet(
    &self,
  ) -> PacketSubscriber<Self::ResolvedAddress, <Self::Runtime as RuntimeLite>::Instant>;

  /// Returns a receiver that can be read to handle incoming stream
  /// connections from other peers. How this is set up for listening is
  /// left as an exercise for the concrete transport implementations.
  fn stream(&self) -> StreamSubscriber<Self::ResolvedAddress, Self::Stream>;

  /// Shutdown the transport
  fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
