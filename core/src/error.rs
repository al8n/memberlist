use std::borrow::Cow;

use nodecraft::{resolver::AddressResolver, Node};
use smol_str::SmolStr;

use crate::{delegate::Delegate, transport::Transport, types::ErrorResponse};

pub use crate::{
  transport::TransportError,
  version::{UnknownDelegateVersion, UnknownProtocolVersion},
};

#[derive(thiserror::Error)]
pub enum Error<T: Transport, D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
{
  #[error("memberlist: node is not running, please bootstrap first")]
  NotRunning,
  #[error("memberlist: timeout waiting for update broadcast")]
  UpdateTimeout,
  #[error("memberlist: timeout waiting for leave broadcast")]
  LeaveTimeout,
  #[error("memberlist: no response from node {0}")]
  Lost(Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  #[error("memberlist: {0}")]
  Delegate(<D as Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>::Error),
  #[error("memberlist: {0}")]
  Transport(T::Error),
  /// Returned when a message is received with an unexpected type.
  #[error("memberlist: unexpected message: expected {expected}, got {got}")]
  UnexpectedMessage {
    /// The expected message type.
    expected: &'static str,
    /// The actual message type.
    got: &'static str,
  },
  /// Returned when the sequence number of [`Ack`](crate::types::Ack) is not
  /// match the sequence number of [`Ping`](crate::types::Ping).
  #[error("memberlist: sequence number mismatch: ping({ping}), ack({ack})")]
  SequenceNumberMismatch {
    /// The sequence number of [`Ping`](crate::types::Ping).
    ping: u32,
    /// The sequence number of [`Ack`](crate::types::Ack).
    ack: u32,
  },
  // #[error("memberlist: {0}")]
  // BlockedAddress(#[from] ForbiddenIp),
  #[error("memberlist: remote error: {0}")]
  Remote(SmolStr),
  #[error("memberlist: {0}")]
  Other(Cow<'static, str>),
}

impl<T: Transport, D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
  core::fmt::Debug for Error<T, D>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{self}")
  }
}

impl<T: Transport, D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
  Error<T, D>
{
  /// Creates a new error with the given delegate error.
  #[inline]
  pub fn delegate(
    e: <D as Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>::Error,
  ) -> Self {
    Self::Delegate(e)
  }

  /// Creates a [`Error::SequenceNumberMismatch`] error.
  #[inline]
  pub fn sequence_number_mismatch(ping: u32, ack: u32) -> Self {
    Self::SequenceNumberMismatch { ping, ack }
  }

  /// Creates a [`Error::UnexpectedMessage`] error.
  #[inline]
  pub fn unexpected_message(expected: &'static str, got: &'static str) -> Self {
    Self::UnexpectedMessage { expected, got }
  }

  /// Creates a new error with the given transport error.
  #[inline]
  pub fn transport(e: T::Error) -> Self {
    Self::Transport(e)
  }

  /// Creates a new error with the given remote error.
  #[inline]
  pub fn remote(e: ErrorResponse) -> Self {
    Self::Remote(e.err)
  }

  /// Creates a new error with the given message.
  #[inline]
  pub fn custom(e: std::borrow::Cow<'static, str>) -> Self {
    Self::Other(e)
  }

  #[inline]
  pub(crate) fn is_remote_failure(&self) -> bool {
    match self {
      Self::Transport(e) => e.is_remote_failure(),
      _ => false,
    }
  }
}
