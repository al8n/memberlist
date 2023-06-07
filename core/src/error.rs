use crate::{
  delegate::Delegate,
  dns::DnsError,
  network::NetworkError,
  options::ForbiddenIp,
  transport::{Transport, TransportError},
  types::{Domain, InvalidMessageType, NodeId},
};

#[derive(thiserror::Error)]
pub enum Error<D: Delegate, T: Transport> {
  #[error("showbiz: invalid message type {0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  #[error("showbiz: remote node state(size {0}) is larger than limit")]
  LargeRemoteState(usize),
  #[error("showbiz: security error {0}")]
  Security(#[from] crate::security::SecurityError),
  #[error("showbiz: keyring error {0}")]
  Keyring(#[from] crate::keyring::SecretKeyringError),
  #[error("showbiz: node names are required by configuration but one was not provided")]
  MissingNodeName,
  #[error("showbiz: {0}")]
  Compression(#[from] crate::util::CompressionError),
  #[error("showbiz: {0}")]
  LocalBroadcast(#[from] async_channel::SendError<()>),
  #[error("showbiz: timeout waiting for update broadcast")]
  UpdateTimeout,
  #[error("showbiz: dns error: {0}")]
  Dns(#[from] trust_dns_resolver::error::ResolveError),
  #[error("showbiz: {0}")]
  Delegate(D::Error),
  #[error("showbiz: {0}")]
  Transport(#[from] TransportError<T>),
  #[error("showbiz: timeout waiting for leave broadcast")]
  LeaveTimeout,
  #[error("showbiz: {0}")]
  ForbiddenIp(#[from] ForbiddenIp),
  #[error("showbiz: cannot parse ip from {0}")]
  ParseIpFailed(Domain),
  #[error("showbiz: network error {0}")]
  Network(#[from] NetworkError<T>),
  #[error("showbiz: no response from node {0}")]
  NoPingResponse(NodeId),
  #[error("showbiz: {0}")]
  Other(String),
  #[error("showbiz: node is not running, please bootstrap first")]
  NotRunning,
}

impl<D: Delegate, T: Transport> core::fmt::Debug for Error<D, T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{self}")
  }
}

impl<D: Delegate, T: Transport> Error<D, T> {
  #[inline]
  pub fn delegate(e: D::Error) -> Self {
    Self::Delegate(e)
  }

  #[inline]
  pub fn transport(e: TransportError<T>) -> Self {
    Self::Transport(e)
  }

  #[inline]
  pub(crate) fn dns_resolve(e: trust_dns_resolver::error::ResolveError) -> Self {
    Self::Transport(TransportError::Dns(DnsError::Resolve(e)))
  }

  #[inline]
  pub(crate) fn failed_remote(&self) -> bool {
    match self {
      Self::Transport(e) => e.failed_remote(),
      _ => false,
    }
  }
}
