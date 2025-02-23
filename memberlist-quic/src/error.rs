use super::*;

/// Errors that can occur when using [`QuicTransport`].
#[derive(thiserror::Error)]
pub enum QuicTransportError<A: AddressResolver> {
  /// Returns when there is no explicit advertise address and no private IP address found.
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  /// Returns when there is no bind address provided.
  #[error("at least one bind address is required")]
  EmptyBindAddresses,
  /// Returns when the ip is blocked.
  #[error("the ip {0} is blocked")]
  BlockedIp(IpAddr),
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
  /// Returns when the using Wire to encode/decode message.
  #[error(transparent)]
  Io(#[from] std::io::Error),
  /// Returns when the packet is too large.
  #[error("packet too large, the maximum packet can be sent is 65535, got {0}")]
  PacketTooLarge(usize),
  /// Returns when there is a custom error.
  #[error("{0}")]
  Custom(std::borrow::Cow<'static, str>),
}

impl<A: AddressResolver> core::fmt::Debug for QuicTransportError<A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A> TransportError for QuicTransportError<A>
where
  A: AddressResolver,
  A::Address: Send + Sync + 'static,
{
  fn is_remote_failure(&self) -> bool {
    use std::io::ErrorKind;

    match &self {
      Self::Io(e) => matches!(
        e.kind(),
        ErrorKind::ConnectionRefused
          | ErrorKind::ConnectionReset
          | ErrorKind::ConnectionAborted
          | ErrorKind::BrokenPipe
          | ErrorKind::TimedOut
          | ErrorKind::NotConnected
      ),
      _ => false,
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}
