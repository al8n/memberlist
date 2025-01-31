use super::*;

/// Errors that can occur when using [`QuicTransport`].
#[derive(thiserror::Error)]
pub enum QuicTransportError<A: AddressResolver, S: StreamLayer<Runtime = A::Runtime>, W: Wire> {
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
  /// Returns when the listener fails to bind.
  #[error("failed to start listener on {0}: {1}")]
  Listen(SocketAddr, std::io::Error),
  /// Returns when the failed to create a resolver for the transport.
  #[error("failed to create resolver: {0}")]
  Resolver(A::Error),
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
  Label(#[from] LabelError),
  /// Returns when the stream layer has error.
  #[error(transparent)]
  Stream(S::Error),
  /// Returns when the using Wire to encode/decode message.
  #[error(transparent)]
  IO(#[from] std::io::Error),

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
  #[error("compressor: {0}")]
  Compressor(#[from] compressor::CompressorError),

  /// Returns when the computation task panic
  #[error("computation task panic")]
  #[cfg(feature = "compression")]
  ComputationTaskFailed,
}

impl<A: AddressResolver, S: StreamLayer<Runtime = A::Runtime>, W: Wire> core::fmt::Debug
  for QuicTransportError<A, S, W>
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A, S, W> TransportError for QuicTransportError<A, S, W>
where
  A: AddressResolver,
  A::Address: Send + Sync + 'static,
  S: StreamLayer<Runtime = A::Runtime>,
  W: Wire,
{
  fn is_remote_failure(&self) -> bool {
    if let Self::Stream(e) = self {
      e.is_remote_failure()
    } else {
      false
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}
