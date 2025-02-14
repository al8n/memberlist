use std::{borrow::Cow, collections::HashMap, sync::Arc};

use memberlist_types::EncoderError;
use nodecraft::{resolver::AddressResolver, Node};
use smallvec_wrapper::OneOrMore;
use smol_str::SmolStr;

use crate::{
  delegate::{Delegate, DelegateError},
  transport::{MaybeResolvedAddress, Transport},
  types::{DecodeError, EncodeError, ErrorResponse, SmallVec},
};

#[cfg(any(
  feature = "crc32",
  feature = "xxhash64",
  feature = "xxhash32",
  feature = "xxhash3",
  feature = "murmur3",
))]
use crate::types::ChecksumError;

#[cfg(any(
  feature = "zstd",
  feature = "lz4",
  feature = "snappy",
  feature = "brotli",
))]
use crate::types::CompressionError;

#[cfg(feature = "encryption")]
pub use crate::types::EncryptionError;

pub use crate::transport::TransportError;

/// Error returned by `Memberlist::join_many`.
pub struct JoinError<T: Transport, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  pub(crate) joined: SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  pub(crate) errors: HashMap<Node<T::Id, MaybeResolvedAddress<T>>, Error<T, D>>,
}

impl<D, T: Transport> From<JoinError<T, D>>
  for (
    SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    HashMap<Node<T::Id, MaybeResolvedAddress<T>>, Error<T, D>>,
  )
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn from(e: JoinError<T, D>) -> Self {
    (e.joined, e.errors)
  }
}

impl<D, T: Transport> core::fmt::Debug for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {:?}", self.joined)?;
    }

    if !self.errors.is_empty() {
      writeln!(f, "Failures:")?;
      for (addr, err) in self.errors.iter() {
        writeln!(f, "\t{}: {}", addr, err)?;
      }
    }

    Ok(())
  }
}

impl<D, T: Transport> core::fmt::Display for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {:?}", self.joined)?;
    }

    if !self.errors.is_empty() {
      writeln!(f, "Failures:")?;
      for (addr, err) in self.errors.iter() {
        writeln!(f, "\t{addr}: {err}")?;
      }
    }

    Ok(())
  }
}

impl<T: Transport, D> std::error::Error for JoinError<T, D> where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>
{
}

impl<T: Transport, D> JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  /// Return the number of successful joined nodes
  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }

  /// Return the joined nodes
  pub const fn joined(
    &self,
  ) -> &SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    &self.joined
  }
}

impl<T: Transport, D> JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  /// Return the errors
  pub const fn errors(&self) -> &HashMap<Node<T::Id, MaybeResolvedAddress<T>>, Error<T, D>> {
    &self.errors
  }
}

/// Error type for the [`Memberlist`](crate::Memberlist)
#[derive(thiserror::Error)]
pub enum Error<T: Transport, D: Delegate> {
  /// Returns when the node is not running.
  #[error("node is not running, please bootstrap first")]
  NotRunning,
  /// Returns when timeout waiting for update broadcast.
  #[error("timeout waiting for update broadcast")]
  UpdateTimeout,
  /// Returns when timeout waiting for leave broadcast.
  #[error("timeout waiting for leave broadcast")]
  LeaveTimeout,
  /// Returns when lost connection with a peer.
  #[error("no response from node {0}")]
  Lost(Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  /// Delegate error
  #[error(transparent)]
  Delegate(#[from] DelegateError<D>),
  /// Transport error
  #[error(transparent)]
  Transport(T::Error),
  /// Returned when a message is received with an unexpected type.
  #[error("unexpected message: expected {expected}, got {got}")]
  UnexpectedMessage {
    /// The expected message type.
    expected: &'static str,
    /// The actual message type.
    got: &'static str,
  },
  /// Returned when the sequence number of [`Ack`](crate::types::Ack) is not
  /// match the sequence number of [`Ping`](crate::types::Ping).
  #[error("sequence number mismatch: ping({ping}), ack({ack})")]
  SequenceNumberMismatch {
    /// The sequence number of [`Ping`](crate::types::Ping).
    ping: u32,
    /// The sequence number of [`Ack`](crate::types::Ack).
    ack: u32,
  },
  /// Failed to encode message
  #[error(transparent)]
  Encode(#[from] EncodeError),
  /// Failed to decode message
  #[error(transparent)]
  Decode(#[from] DecodeError),
  /// Returned when a remote error is received.
  #[error("remote error: {0}")]
  Remote(SmolStr),
  /// Multiple errors
  #[error("errors:\n{}", format_multiple_errors(.0))]
  Multiple(Arc<[Self]>),
  /// Returned when a custom error is created by users.
  #[error("{0}")]
  Other(Cow<'static, str>),
  /// Encryption error
  #[error(transparent)]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  Encryption(#[from] EncryptionError),
  /// Compressor error
  #[error(transparent)]
  #[cfg(any(
    feature = "zstd",
    feature = "lz4",
    feature = "snappy",
    feature = "brotli",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "zstd",
      feature = "lz4",
      feature = "snappy",
      feature = "brotli",
    )))
  )]
  Compression(#[from] CompressionError),
  /// Checksum error
  #[error(transparent)]
  #[cfg(any(
    feature = "crc32",
    feature = "xxhash64",
    feature = "xxhash32",
    feature = "xxhash3",
    feature = "murmur3",
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash64",
      feature = "xxhash32",
      feature = "xxhash3",
      feature = "murmur3",
    )))
  )]
  Checksum(#[from] ChecksumError),
}

impl<T: Transport, D: Delegate> core::fmt::Debug for Error<T, D> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{self}")
  }
}

impl<T: Transport, D: Delegate> From<EncoderError> for Error<T, D> {
  fn from(value: EncoderError) -> Self {
    match value {
      #[cfg(any(
        feature = "zstd",
        feature = "lz4",
        feature = "snappy",
        feature = "brotli",
      ))]
      EncoderError::Compress(e) => Self::Compression(e),
      #[cfg(any(
        feature = "crc32",
        feature = "xxhash64",
        feature = "xxhash32",
        feature = "xxhash3",
        feature = "murmur3",
      ))]
      EncoderError::Checksum(e) => Self::Checksum(e),
      #[cfg(feature = "encryption")]
      EncoderError::Encrypt(e) => Self::Encryption(e),
      EncoderError::Encode(e) => Self::Encode(e),
    }
  }
}

impl<T: Transport, D: Delegate> FromIterator<Error<T, D>> for Option<Error<T, D>> {
  fn from_iter<I: IntoIterator<Item = Error<T, D>>>(iter: I) -> Self {
    let errors = iter.into_iter().collect::<OneOrMore<_>>();
    let num_errs = errors.len();
    match num_errs {
      0 => None,
      _ => Some(match errors.into_either() {
        either::Either::Left([e]) => e,
        either::Either::Right(e) => Error::Multiple(e.into_vec().into()),
      }),
    }
  }
}

impl<T: Transport, D: Delegate> Error<T, D> {
  /// Creates a new error with the given delegate error.
  #[inline]
  pub fn delegate(e: DelegateError<D>) -> Self {
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
    Self::Remote(e.into())
  }

  /// Creates a new error with the given message.
  #[inline]
  pub fn custom(e: std::borrow::Cow<'static, str>) -> Self {
    Self::Other(e)
  }

  #[inline]
  pub(crate) async fn try_from_stream<S>(stream: S) -> Option<Self>
  where
    S: futures::stream::Stream<Item = Self>,
  {
    use futures::stream::StreamExt;

    let errs = stream.collect::<OneOrMore<_>>().await;

    let num = errs.len();
    match num {
      0 => None,
      _ => Some(match errs.into_either() {
        either::Either::Left([e]) => e,
        either::Either::Right(e) => Self::Multiple(e.into_vec().into()),
      }),
    }
  }

  #[inline]
  pub(crate) fn is_remote_failure(&self) -> bool {
    match self {
      Self::Transport(e) => e.is_remote_failure(),
      Self::Multiple(errors) => errors.iter().any(Self::is_remote_failure),
      _ => false,
    }
  }
}

fn format_multiple_errors<T: Transport, D: Delegate>(errors: &[Error<T, D>]) -> String {
  errors
    .iter()
    .enumerate()
    .map(|(i, err)| format!("  {}. {}", i + 1, err))
    .collect::<Vec<_>>()
    .join("\n")
}
