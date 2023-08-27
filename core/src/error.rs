use std::borrow::Cow;

use rkyv::de::deserializers::SharedDeserializeMapError;

use crate::{delegate::Delegate, dns::DnsError, transport::Transport, types::NodeId};

pub use crate::{
  options::ForbiddenIp,
  security::{SecurityError, UnknownEncryptionAlgo},
  transport::TransportError,
  types::{CompressError, DecodeError, DecompressError, EncodeError, InvalidDomain, InvalidLabel},
  version::{InvalidDelegateVersion, InvalidProtocolVersion},
};

#[derive(thiserror::Error)]
pub enum Error<T: Transport, D: Delegate> {
  #[error("showbiz: node is not running, please bootstrap first")]
  NotRunning,
  #[error("showbiz: timeout waiting for update broadcast")]
  UpdateTimeout,
  #[error("showbiz: timeout waiting for leave broadcast")]
  LeaveTimeout,
  #[error("showbiz: no response from node {0}")]
  NoPingResponse(NodeId),
  #[error("showbiz: {0}")]
  Delegate(D::Error),
  #[error("showbiz: {0}")]
  Transport(#[from] TransportError<T>),
  #[error("showbiz: {0}")]
  ForbiddenIp(#[from] ForbiddenIp),
  #[error("showbiz: peer error: {0}")]
  Peer(String),
  #[error("showbiz: offload thread panic, fail to receive offload thread message")]
  OffloadPanic,
  #[error("showbiz: {0}")]
  Other(Cow<'static, str>),
}

impl<D: Delegate, T: Transport> From<SharedDeserializeMapError> for Error<T, D> {
  #[inline]
  fn from(e: SharedDeserializeMapError) -> Self {
    Self::Transport(DecodeError::Decode(e).into())
  }
}

impl<D: Delegate, T: Transport> From<CompressError> for Error<T, D> {
  #[inline]
  fn from(e: CompressError) -> Self {
    Self::Transport(DecodeError::Compress(e).into())
  }
}

impl<D: Delegate, T: Transport> From<DecompressError> for Error<T, D> {
  #[inline]
  fn from(e: DecompressError) -> Self {
    Self::Transport(DecodeError::Decompress(e).into())
  }
}

impl<D: Delegate, T: Transport> From<crate::security::SecurityError> for Error<T, D> {
  #[inline]
  fn from(e: crate::security::SecurityError) -> Self {
    Self::Transport(e.into())
  }
}

impl<D: Delegate, T: Transport> core::fmt::Debug for Error<T, D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{self}")
  }
}

impl<D: Delegate, T: Transport> From<crate::types::DecodeError> for Error<T, D> {
  fn from(e: crate::types::DecodeError) -> Self {
    Self::Transport(TransportError::Decode(e))
  }
}

impl<D: Delegate, T: Transport> Error<T, D> {
  #[inline]
  pub fn delegate(e: D::Error) -> Self {
    Self::Delegate(e)
  }

  #[inline]
  pub fn transport(e: TransportError<T>) -> Self {
    Self::Transport(e)
  }

  #[inline]
  pub fn other(e: std::borrow::Cow<'static, str>) -> Self {
    Self::Other(e)
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
