use std::net::{IpAddr, SocketAddr};

use memberlist_core::transport::{TransportError, Wire};
use nodecraft::resolver::AddressResolver;

/// Errors that can occur when using [`NetTransport`](super::NetTransport).
#[derive(thiserror::Error)]
pub enum NetTransportError<A: AddressResolver, W: Wire> {
  /// Connection error.
  #[error(transparent)]
  Connection(#[from] ConnectionError),
  /// Returns when there is no explicit advertise address and no private IP address found.
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  /// Returns when there is no bind address provided.
  #[error("at least one bind address is required")]
  EmptyBindAddresses,
  /// Returns when the ip is blocked.
  #[error("the ip {0} is blocked")]
  BlockedIp(IpAddr),
  /// Returns when the packet buffer size is too small.
  #[error("failed to resize packet buffer {0}")]
  ResizePacketBuffer(std::io::Error),
  /// Returns when the packet socket fails to bind.
  #[error("failed to start packet listener on {0}: {1}")]
  ListenPacket(SocketAddr, std::io::Error),
  /// Returns when the promised listener fails to bind.
  #[error("failed to start promised listener on {0}: {1}")]
  ListenPromised(SocketAddr, std::io::Error),
  /// Returns when the failed to create a resolver for the transport.
  #[error("failed to create resolver: {0}")]
  Resolver(A::Error),
  /// Returns when the failed to create a stream layer for the transport.
  #[error("failed to create stream layer: {0}")]
  StreamLayer(std::io::Error),
  /// Returns when we fail to resolve an address.
  #[error("failed to resolve address {addr}: {err}")]
  Resolve {
    /// The address we failed to resolve.
    addr: A::Address,
    /// The error that occurred.
    err: A::Error,
  },
  /// Returns when the label error.
  #[error(transparent)]
  Label(#[from] super::LabelError),

  /// Returns when the checksum of the message bytes comes from the packet stream does not
  /// match the original checksum.
  #[error("checksum mismatch")]
  PacketChecksumMismatch,
  /// Returns when getting unkstartn checksumer
  #[error(transparent)]
  UnknownChecksumer(#[from] super::checksum::UnknownChecksumer),
  /// Returns when encode/decode error.
  #[error("wire error: {0}")]
  Wire(W::Error),
  /// Returns when the packet is too large.
  #[error("packet too large, the maximum packet can be sent is 65535, got {0}")]
  PacketTooLarge(usize),
  /// Returns when there is a custom error.
  #[error("custom error: {0}")]
  Custom(std::borrow::Cow<'static, str>),

  /// Returns when fail to compress/decompress message.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error(transparent)]
  Compressor(#[from] super::compressor::CompressorError),
  /// Returns when there is a security error. e.g. encryption/decryption error.
  #[error(transparent)]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  Security(#[from] super::security::SecurityError),

  /// Returns when the computation task panic
  #[error("computation task panic")]
  #[cfg(any(feature = "compression", feature = "encryption"))]
  ComputationTaskFailed,
}

#[cfg(feature = "compression")]
const _: () = {
  use super::compressor::*;

  impl<A: AddressResolver, W: Wire> From<CompressError> for NetTransportError<A, W> {
    fn from(err: CompressError) -> Self {
      Self::Compressor(err.into())
    }
  }

  impl<A: AddressResolver, W: Wire> From<DecompressError> for NetTransportError<A, W> {
    fn from(err: DecompressError) -> Self {
      Self::Compressor(err.into())
    }
  }

  impl<A: AddressResolver, W: Wire> From<UnknownCompressor> for NetTransportError<A, W> {
    fn from(err: UnknownCompressor) -> Self {
      Self::Compressor(err.into())
    }
  }
};

impl<A: AddressResolver, W: Wire> NetTransportError<A, W> {
  #[cfg(feature = "compression")]
  pub(crate) fn not_enough_bytes_to_decompress() -> Self {
    Self::Compressor(super::compressor::CompressorError::NotEnoughBytes)
  }
}

impl<A: AddressResolver, W: Wire> core::fmt::Debug for NetTransportError<A, W> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A, W> TransportError for NetTransportError<A, W>
where
  A: AddressResolver,
  A::Address: Send + Sync + 'static,
  W: Wire,
{
  fn is_remote_failure(&self) -> bool {
    if let Self::Connection(e) = self {
      e.is_remote_failure()
    } else {
      false
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}

/// Connection kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum ConnectionKind {
  /// Promised connection, e.g. TCP, QUIC.
  Promised,
  /// Packet connection, e.g. UDP.
  Packet,
}

impl core::fmt::Display for ConnectionKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionKind {
  /// Returns a string representation of the connection kind.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      ConnectionKind::Promised => "promised",
      ConnectionKind::Packet => "packet",
    }
  }
}

/// Connection error kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum ConnectionErrorKind {
  /// Failed to accept a connection.
  Accept,
  /// Failed to close a connection.
  Close,
  /// Failed to dial a connection.
  Dial,
  /// Failed to flush a connection.
  Flush,
  /// Failed to read from a connection.
  Read,
  /// Failed to write to a connection.
  Write,
  /// Failed to set a label on a connection.
  Label,
}

impl core::fmt::Display for ConnectionErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionErrorKind {
  /// Returns a string representation of the error kind.
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Accept => "accept",
      Self::Read => "read",
      Self::Write => "write",
      Self::Dial => "dial",
      Self::Flush => "flush",
      Self::Close => "close",
      Self::Label => "label",
    }
  }
}

/// Connection error.
#[derive(Debug)]
pub struct ConnectionError {
  pub(crate) kind: ConnectionKind,
  pub(crate) error_kind: ConnectionErrorKind,
  pub(crate) error: std::io::Error,
}

impl core::fmt::Display for ConnectionError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "{} connection {} error {}",
      self.kind.as_str(),
      self.error_kind.as_str(),
      self.error
    )
  }
}

impl std::error::Error for ConnectionError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    Some(&self.error)
  }
}

impl From<ConnectionError> for std::io::Error {
  fn from(value: ConnectionError) -> Self {
    value.error
  }
}

impl ConnectionError {
  /// Returns true if the error is a remote failure.
  #[inline]
  pub fn is_remote_failure(&self) -> bool {
    #[allow(clippy::match_like_matches_macro)]
    match self.kind {
      ConnectionKind::Promised => match self.error_kind {
        ConnectionErrorKind::Read | ConnectionErrorKind::Write | ConnectionErrorKind::Dial => true,
        _ => false,
      },
      ConnectionKind::Packet => match self.error_kind {
        ConnectionErrorKind::Read | ConnectionErrorKind::Write => true,
        _ => false,
      },
    }
  }

  pub(super) fn promised_read(err: std::io::Error) -> Self {
    Self {
      kind: ConnectionKind::Promised,
      error_kind: ConnectionErrorKind::Read,
      error: err,
    }
  }

  pub(super) fn promised_write(err: std::io::Error) -> Self {
    Self {
      kind: ConnectionKind::Promised,
      error_kind: ConnectionErrorKind::Write,
      error: err,
    }
  }

  pub(super) fn packet_write(err: std::io::Error) -> Self {
    Self {
      kind: ConnectionKind::Packet,
      error_kind: ConnectionErrorKind::Write,
      error: err,
    }
  }

  pub(super) fn packet_write_on_transport_shutdown(err: std::io::Error) -> Self {
    Self {
      kind: ConnectionKind::Packet,
      error_kind: ConnectionErrorKind::Close,
      error: err,
    }
  }
}
