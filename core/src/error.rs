use std::borrow::Cow;

use nodecraft::{resolver::AddressResolver, Node};
use rkyv::de::deserializers::SharedDeserializeMapError;

use crate::{delegate::Delegate, transport::Transport};

pub use crate::{
  options::ForbiddenIp,
  security::{SecurityError, UnknownEncryptionAlgo},
  transport::TransportError,
  types::{CompressError, DecodeError, DecompressError, EncodeError, InvalidLabel},
  version::{InvalidDelegateVersion, InvalidProtocolVersion},
};

#[derive(thiserror::Error)]
pub enum Error<
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>,
> {
  #[error("showbiz: node is not running, please bootstrap first")]
  NotRunning,
  #[error("showbiz: timeout waiting for update broadcast")]
  UpdateTimeout,
  #[error("showbiz: timeout waiting for leave broadcast")]
  LeaveTimeout,
  #[error("showbiz: no response from node {0}")]
  NoPingResponse(Node<T::Id, <T::Resolver as AddressResolver>::Address>),
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

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  From<SharedDeserializeMapError> for Error<T, D>
{
  #[inline]
  fn from(e: SharedDeserializeMapError) -> Self {
    Self::Transport(DecodeError::Decode(e).into())
  }
}

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  From<CompressError> for Error<T, D>
{
  #[inline]
  fn from(e: CompressError) -> Self {
    Self::Transport(DecodeError::Compress(e).into())
  }
}

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  From<DecompressError> for Error<T, D>
{
  #[inline]
  fn from(e: DecompressError) -> Self {
    Self::Transport(DecodeError::Decompress(e).into())
  }
}

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  From<crate::security::SecurityError> for Error<T, D>
{
  #[inline]
  fn from(e: crate::security::SecurityError) -> Self {
    Self::Transport(e.into())
  }
}

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  core::fmt::Debug for Error<T, D>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{self}")
  }
}

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  From<crate::types::DecodeError> for Error<T, D>
{
  fn from(e: crate::types::DecodeError) -> Self {
    Self::Transport(TransportError::Decode(e))
  }
}

impl<T: Transport, D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::Address>>
  Error<T, D>
{
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
  pub(crate) fn failed_remote(&self) -> bool {
    match self {
      Self::Transport(e) => e.failed_remote(),
      _ => false,
    }
  }
}
