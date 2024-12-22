use std::marker::PhantomData;

use nodecraft::Id;

use crate::transport::{TimeoutableReadStream, TimeoutableWriteStream, TransportError, Wire};

use super::*;

/// An error type for the [`UnimplementedTransport`].
#[derive(Debug, thiserror::Error)]
#[error("error for unimplemented transport")]
pub struct UnimplementedTransportError;

impl TransportError for UnimplementedTransportError {
  fn is_remote_failure(&self) -> bool {
    unimplemented!()
  }

  fn custom(_err: std::borrow::Cow<'static, str>) -> Self {
    unimplemented!()
  }
}

/// A stream that does not implement any of the required methods.
/// Which can be only used for testing purposes.
pub struct UnimplementedStream;

impl TimeoutableReadStream for UnimplementedStream {
  fn set_read_deadline(&mut self, _: Option<Instant>) {
    unimplemented!()
  }

  fn read_deadline(&self) -> Option<Instant> {
    unimplemented!()
  }
}

impl TimeoutableWriteStream for UnimplementedStream {
  fn set_write_deadline(&mut self, _: Option<Instant>) {
    unimplemented!()
  }

  fn write_deadline(&self) -> Option<Instant> {
    unimplemented!()
  }
}

/// A transport that does not implement any of the required methods.
/// Which can be only used for testing purposes.
pub struct UnimplementedTransport<I, A, W, R>(PhantomData<(I, A, W, R)>);

impl<I, A, W, R> Transport for UnimplementedTransport<I, A, W, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<Runtime = R>,
  A::Address: Send + Sync + 'static,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: RuntimeLite,
{
  type Error = UnimplementedTransportError;

  type Id = I;

  type Resolver = A;

  type Stream = UnimplementedStream;

  type Wire = W;

  type Runtime = R;

  type Options = ();

  async fn new(_: Self::Options) -> Result<Self, Self::Error> {
    unimplemented!()
  }

  async fn resolve(
    &self,
    _: &<Self::Resolver as AddressResolver>::Address,
  ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
    unimplemented!()
  }

  fn local_id(&self) -> &Self::Id {
    unimplemented!()
  }

  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    unimplemented!()
  }

  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    unimplemented!()
  }

  #[cfg(feature = "encryption")]
  fn keyring(&self) -> Option<&memberlist_types::SecretKeyring> {
    unimplemented!()
  }

  #[cfg(feature = "encryption")]
  fn encryption_enabled(&self) -> bool {
    unimplemented!()
  }

  fn max_payload_size(&self) -> usize {
    unimplemented!()
  }

  fn packets_header_overhead(&self) -> usize {
    unimplemented!()
  }

  fn packet_overhead(&self) -> usize {
    unimplemented!()
  }

  fn blocked_address(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error> {
    unimplemented!()
  }

  async fn read_message(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    _: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    unimplemented!()
  }

  async fn send_message(
    &self,
    _: &mut Self::Stream,
    _: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    unimplemented!()
  }

  async fn send_packet(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    _: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, Instant), Self::Error> {
    unimplemented!()
  }

  async fn send_packets(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    _: memberlist_types::TinyVec<
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    >,
  ) -> Result<(usize, Instant), Self::Error> {
    unimplemented!()
  }

  async fn dial_with_deadline(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    _: Instant,
  ) -> Result<Self::Stream, Self::Error> {
    unimplemented!()
  }

  async fn cache_stream(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    _: Self::Stream,
  ) -> Result<(), Self::Error> {
    unimplemented!()
  }

  fn packet(
    &self,
  ) -> crate::transport::PacketSubscriber<
    Self::Id,
    <Self::Resolver as AddressResolver>::ResolvedAddress,
  > {
    unimplemented!()
  }

  fn stream(
    &self,
  ) -> crate::transport::StreamSubscriber<
    <Self::Resolver as AddressResolver>::ResolvedAddress,
    Self::Stream,
  > {
    unimplemented!()
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    unimplemented!()
  }
}
