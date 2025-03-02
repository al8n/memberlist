use std::marker::PhantomData;

use nodecraft::Id;

use crate::transport::TransportError;

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

impl From<std::io::Error> for UnimplementedTransportError {
  fn from(_: std::io::Error) -> Self {
    unimplemented!()
  }
}

/// Unimplemented reader for testing purposes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnimplementedReader;

impl ProtoReader for UnimplementedReader {
  async fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn read_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn peek(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn peek_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }
}

/// Unimplemented writer for testing purposes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnimplementedWriter;

impl ProtoWriter for UnimplementedWriter {
  async fn write_all(&mut self, _: &[u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn close(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }
}

/// An unimplemented connection for testing purposes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnimplementedConnection;

impl Connection for UnimplementedConnection {
  type Reader = UnimplementedReader;
  type Writer = UnimplementedWriter;

  fn split(self) -> (Self::Reader, Self::Writer) {
    unimplemented!()
  }

  async fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn read_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn peek(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
    unimplemented!()
  }

  async fn peek_exact(&mut self, _: &mut [u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  fn consume_peek(&mut self) {
    unimplemented!()
  }

  async fn write_all(&mut self, _: &[u8]) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }

  async fn close(&mut self) -> std::io::Result<()> {
    unimplemented!()
  }
}

/// A stream that does not implement any of the required methods.
/// Which can be only used for testing purposes.
pub struct UnimplementedStream<R>(PhantomData<R>);

/// A transport that does not implement any of the required methods.
/// Which can be only used for testing purposes.
pub struct UnimplementedTransport<I, A, R>(PhantomData<(I, A, R)>);

impl<I, A, R> Transport for UnimplementedTransport<I, A, R>
where
  I: Id + Data + Send + Sync + 'static,
  A: AddressResolver<Runtime = R>,
  A::Address: Send + Sync + 'static,
  A::ResolvedAddress: Data + Send + Sync + 'static,
  R: RuntimeLite,
{
  type Error = UnimplementedTransportError;

  type Id = I;

  type Address = A::Address;

  type ResolvedAddress = A::ResolvedAddress;

  type Resolver = A;

  type Connection = UnimplementedConnection;

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

  fn max_packet_size(&self) -> usize {
    unimplemented!()
  }

  fn header_overhead(&self) -> usize {
    unimplemented!()
  }

  fn blocked_address(
    &self,
    _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error> {
    unimplemented!()
  }

  async fn send_to(
    &self,
    _: &Self::ResolvedAddress,
    _: Payload,
  ) -> Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error> {
    unimplemented!()
  }

  async fn open(
    &self,
    _: &Self::ResolvedAddress,
    _: <Self::Runtime as RuntimeLite>::Instant,
  ) -> Result<Self::Connection, Self::Error> {
    unimplemented!()
  }

  fn packet(
    &self,
  ) -> crate::transport::PacketSubscriber<
    <Self::Resolver as AddressResolver>::ResolvedAddress,
    <Self::Runtime as RuntimeLite>::Instant,
  > {
    unimplemented!()
  }

  fn stream(&self) -> StreamSubscriber<Self::ResolvedAddress, Self::Connection> {
    unimplemented!()
  }

  fn packet_reliable(&self) -> bool {
    unimplemented!()
  }

  fn packet_secure(&self) -> bool {
    unimplemented!()
  }

  fn stream_secure(&self) -> bool {
    unimplemented!()
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    unimplemented!()
  }
}
