// use std::marker::PhantomData;

// use memberlist_types::Data;
// use nodecraft::Id;

// use crate::transport::{TimeoutableReadStream, TimeoutableWriteStream, TransportError};

// use super::*;

// /// An error type for the [`UnimplementedTransport`].
// #[derive(Debug, thiserror::Error)]
// #[error("error for unimplemented transport")]
// pub struct UnimplementedTransportError;

// impl TransportError for UnimplementedTransportError {
//   fn is_remote_failure(&self) -> bool {
//     unimplemented!()
//   }

//   fn custom(_err: std::borrow::Cow<'static, str>) -> Self {
//     unimplemented!()
//   }
// }

// /// A stream that does not implement any of the required methods.
// /// Which can be only used for testing purposes.
// pub struct UnimplementedStream<R>(PhantomData<R>);

// impl<R: RuntimeLite> TimeoutableReadStream for UnimplementedStream<R> {
//   type Instant = R::Instant;

//   fn set_read_deadline(&mut self, _: Option<Self::Instant>) {
//     unimplemented!()
//   }

//   fn read_deadline(&self) -> Option<Self::Instant> {
//     unimplemented!()
//   }
// }

// impl<R: RuntimeLite> TimeoutableWriteStream for UnimplementedStream<R> {
//   type Instant = R::Instant;

//   fn set_write_deadline(&mut self, _: Option<Self::Instant>) {
//     unimplemented!()
//   }

//   fn write_deadline(&self) -> Option<Self::Instant> {
//     unimplemented!()
//   }
// }

// /// A transport that does not implement any of the required methods.
// /// Which can be only used for testing purposes.
// pub struct UnimplementedTransport<I, A, R>(PhantomData<(I, A, R)>);

// impl<I, A, R> Transport for UnimplementedTransport<I, A, R>
// where
//   I: Id + Data + Send + Sync + 'static,
//   A: AddressResolver<Runtime = R>,
//   A::Address: Send + Sync + 'static,
//   A::ResolvedAddress: Data + Send + Sync + 'static,
//   R: RuntimeLite,
// {
//   type Error = UnimplementedTransportError;

//   type Id = I;
//   type Address = A::Address;
//   type ResolvedAddress = A::ResolvedAddress;

//   type Resolver = A;

//   type Stream = UnimplementedStream<R>;

//   type Runtime = R;

//   type Options = ();

//   async fn new(_: Self::Options) -> Result<Self, Self::Error> {
//     unimplemented!()
//   }

//   async fn resolve(
//     &self,
//     _: &<Self::Resolver as AddressResolver>::Address,
//   ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
//     unimplemented!()
//   }

//   fn local_id(&self) -> &Self::Id {
//     unimplemented!()
//   }

//   fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
//     unimplemented!()
//   }

//   fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
//     unimplemented!()
//   }

//   #[cfg(feature = "encryption")]
//   fn keyring(&self) -> Option<&crate::keyring::Keyring> {
//     unimplemented!()
//   }

//   #[cfg(feature = "encryption")]
//   fn encryption_enabled(&self) -> bool {
//     unimplemented!()
//   }

//   fn max_packet_size(&self) -> usize {
//     unimplemented!()
//   }

//   fn packets_header_overhead(&self) -> usize {
//     unimplemented!()
//   }

//   fn packet_overhead(&self) -> usize {
//     unimplemented!()
//   }

//   fn blocked_address(
//     &self,
//     _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
//   ) -> Result<(), Self::Error> {
//     unimplemented!()
//   }

//   async fn read(
//     &self,
//     _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
//     _: &mut Self::Stream,
//   ) -> Result<usize, Self::Error> {
//     unimplemented!()
//   }

//   async fn write(&self, _: &mut Self::Stream, _: Bytes) -> Result<usize, Self::Error> {
//     unimplemented!()
//   }

//   async fn send_to(
//     &self,
//     _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
//     _: Bytes,
//   ) -> Result<(usize, R::Instant), Self::Error> {
//     unimplemented!()
//   }

//   async fn dial_with_deadline(
//     &self,
//     _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
//     _: R::Instant,
//   ) -> Result<Self::Stream, Self::Error> {
//     unimplemented!()
//   }

//   async fn close(
//     &self,
//     _: &<Self::Resolver as AddressResolver>::ResolvedAddress,
//     _: Self::Stream,
//   ) -> Result<(), Self::Error> {
//     unimplemented!()
//   }

//   fn packet(
//     &self,
//   ) -> crate::transport::PacketSubscriber<
//     <Self::Resolver as AddressResolver>::ResolvedAddress,
//     <Self::Runtime as RuntimeLite>::Instant,
//   > {
//     unimplemented!()
//   }

//   fn stream(
//     &self,
//   ) -> crate::transport::StreamSubscriber<
//     <Self::Resolver as AddressResolver>::ResolvedAddress,
//     Self::Stream,
//   > {
//     unimplemented!()
//   }

//   fn packet_reliable(&self) -> bool {
//     unimplemented!()
//   }

//   fn packet_secure(&self) -> bool {
//     unimplemented!()
//   }

//   fn stream_secure(&self) -> bool {
//     unimplemented!()
//   }

//   async fn shutdown(&self) -> Result<(), Self::Error> {
//     unimplemented!()
//   }
// }
