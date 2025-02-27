use core::future::Future;

use agnostic_lite::RuntimeLite;
use bytes::Bytes;
pub use nodecraft::{CheapClone, resolver::AddressResolver, *};

use crate::proto::*;

use super::*;

mod stream;
pub use stream::*;

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
  pub const fn resolved(addr: T::ResolvedAddress) -> Self {
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
  pub fn as_resolved(&self) -> Option<&T::ResolvedAddress> {
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
  ) -> Option<&mut T::ResolvedAddress> {
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
  pub fn into_resolved(self) -> Option<T::ResolvedAddress> {
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

/// The connection
pub trait Connection: Send + Sync {
  /// The reader of the connection
  type Reader: ProtoReader + Unpin;
  /// The writer of the connection
  type Writer: ProtoWriter + Unpin;

  /// Splits the connection into a reader and a writer
  fn split(self) -> (Self::Reader, Self::Writer);

  /// Read the payload from the proto reader to the buffer
  ///
  /// Returns the number of bytes read
  fn read(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send;

  /// Read exactly the payload from the proto reader to the buffer
  fn read_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Peek the payload from the proto reader to the buffer
  ///
  /// Returns the number of bytes peeked
  fn peek(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<usize>> + Send;

  /// Peek exactly the payload from the proto reader to the buffer
  fn peek_exact(&mut self, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Consume the content in peek buffer
  fn consume_peek(&mut self);

  /// Write the payload to the proto writer
  fn write_all(&mut self, payload: &[u8]) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Flush the proto writer
  fn flush(&mut self) -> impl Future<Output = std::io::Result<()>> + Send;

  /// Close the proto writer
  fn close(&mut self) -> impl Future<Output = std::io::Result<()>> + Send;
}

/// An error for the transport layer.
pub trait TransportError: From<std::io::Error> + std::error::Error + Send + Sync + 'static {
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

  /// The promised connection used to send and receive messages
  type Connection: Connection + Send + Sync + 'static;

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

  /// Returns the maximum size of a packet that can be sent
  fn max_packet_size(&self) -> usize;

  /// Returns the header overhead in bytes this transport.
  fn header_overhead(&self) -> usize;

  /// Returns an error if the given address is blocked
  fn blocked_address(&self, addr: &Self::ResolvedAddress) -> Result<(), Self::Error>;

  /// This is a hook that allows the transport to perform any extra work
  /// when the memberlist accepts a new connection from a remote node.
  ///
  /// This method will be invoked before the [`Memberlist`] starts to process the data owned
  /// by the memberlist protocol.
  ///
  /// Nomally, the implementation should do nothing, just return the given conn
  /// back to the caller.
  ///
  /// If the transport sends extra data (e.g. a header) in [`Transport::write`], then in this method,
  /// the implementor should consume the extra data sent by the remote node.
  ///
  /// e.g.
  ///
  /// If in [`Transport::write`], the implementor prepends a header to the payload,
  /// then in this method, the implementor should consume the header.
  fn read(
    &self,
    from: &Self::ResolvedAddress,
    conn: &mut <Self::Connection as Connection>::Reader,
  ) -> impl Future<Output = Result<usize, Self::Error>> + Send {
    async move {
      let _from = from;
      let _conn = conn;
      Ok(0)
    }
  }

  /// Sends a message to the remote node by promised connection.
  ///
  /// The `payload` has already been processed by [`ProtoEncoder`].
  ///
  /// The implementor can send more data to the remote node,
  /// but it must before sending the `payload`.
  ///
  /// Returns the number of bytes sent.
  fn write(
    &self,
    conn: &mut <Self::Connection as Connection>::Writer,
    payload: Payload,
  ) -> impl Future<Output = Result<usize, Self::Error>> + Send {
    async move {
      let src = payload.as_slice();
      let len = src.len();
      conn.write_all(src).await?;
      conn.flush().await?;

      Ok(len)
    }
  }

  /// A packet-oriented interface that fires off the given
  /// payload to the given address in a connectionless fashion.
  ///
  /// The payload is not compressed or encrypted, the transport
  /// layer is expected to handle that by themselves.
  ///
  /// # Returns
  ///
  /// - number of bytes sent
  /// - a time stamp that's as close as possible to when the packet
  ///   was transmitted to help make accurate RTT measurements during probes.
  fn send_to(
    &self,
    addr: &Self::ResolvedAddress,
    packet: Payload,
  ) -> impl Future<Output = Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error>> + Send;

  /// Used to create a bidirection connection that allows us to perform
  /// two-way communication with a peer. This is generally more expensive
  /// than packet connections so is used for more infrequent operations
  /// such as anti-entropy or fallback probes if the packet-oriented probe
  /// failed.
  fn open(
    &self,
    addr: &Self::ResolvedAddress,
    deadline: <Self::Runtime as RuntimeLite>::Instant,
  ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

  /// Returns a packet subscriber that can be used to receive incoming packets
  fn packet(
    &self,
  ) -> PacketSubscriber<Self::ResolvedAddress, <Self::Runtime as RuntimeLite>::Instant>;

  /// Returns a receiver that can be read to handle incoming stream
  /// connections from other peers. How this is set up for listening is
  /// left as an exercise for the concrete transport implementations.
  fn stream(&self) -> StreamSubscriber<Self::ResolvedAddress, Self::Connection>;

  /// Returns `true` if the transport provides provides reliable packets delivery.
  ///
  /// When `true`, the [`Memberlist`] will not include checksums in packets
  /// even if a [`ChecksumAlgorithm`] is configured in [`Options`],
  /// since the transport already guarantees data integrity.
  ///
  /// # Examples
  ///
  /// - Reliable: TCP, QUIC
  /// - Unreliable: UDP
  fn packet_reliable(&self) -> bool;

  /// Returns `true` if the transport provides packets security.
  ///
  /// When `true`, the [`Memberlist`] will not perform additional payload
  /// encryption even if a [`EncryptionAlgorithm`] is configured in [`Options`],
  /// since the transport already provides packet security.
  ///
  /// # Examples
  ///
  /// - Secure: QUIC, TLS
  /// - Insecure: TCP, UDP
  fn packet_secure(&self) -> bool;

  /// Returns `true` if the transport provides stream security.
  ///
  /// When `true`, the [`Memberlist`] will not perform additional payload
  /// encryption even if a [`EncryptionAlgorithm`] is configured in [`Options`],
  /// since the transport already provides stream security.
  ///
  /// # Examples
  ///
  /// - Secure: QUIC, TLS
  /// - Insecure: TCP
  fn stream_secure(&self) -> bool;

  /// Shutdown the transport
  fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
