use async_channel::{Receiver, Sender};
pub use async_channel::{RecvError, SendError, TryRecvError, TrySendError};
use futures::Stream;

use super::*;

/// The packet receives from the unreliable connection.
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Packet<A, T> {
  /// The raw contents of the packet.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the messages of the packet")
    ),
    setter(attrs(doc = "Sets the messages of the packet (Builder pattern)"))
  )]
  payload: Vec<u8>,

  /// Address of the peer. This is an actual address so we
  /// can expose some concrete details about incoming packets.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the address sent the packet")
    ),
    setter(attrs(doc = "Sets the address who sent the packet (Builder pattern)"))
  )]
  from: A,

  /// The time when the packet was received. This should be
  /// taken as close as possible to the actual receipt time to help make an
  /// accurate RTT measurement during probes.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the instant when the packet was received")
    ),
    setter(attrs(doc = "Sets the instant when the packet was received (Builder pattern)"))
  )]
  timestamp: T,
}

impl<A, T> Packet<A, T> {
  /// Create a new packet
  #[inline]
  pub const fn new(from: A, timestamp: T, payload: Vec<u8>) -> Self {
    Self {
      payload,
      from,
      timestamp,
    }
  }

  /// Returns the raw contents of the packet.
  #[inline]
  pub fn into_components(self) -> (A, T, Vec<u8>) {
    (self.from, self.timestamp, self.payload)
  }

  /// Sets the address who sent the packet
  #[inline]
  pub fn set_from(&mut self, from: A) -> &mut Self {
    self.from = from;
    self
  }

  /// Sets the instant when the packet was received
  #[inline]
  pub fn set_timestamp(&mut self, timestamp: T) -> &mut Self {
    self.timestamp = timestamp;
    self
  }

  /// Sets the payload of the packet
  #[inline]
  pub fn set_payload(&mut self, payload: Vec<u8>) -> &mut Self {
    self.payload = payload;
    self
  }
}

/// A producer for packets.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketProducer<A, T> {
  sender: Sender<Packet<A, T>>,
}

/// A subscriber for packets.
#[pin_project::pin_project]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketSubscriber<A, T> {
  #[pin]
  receiver: Receiver<Packet<A, T>>,
}

/// Returns producer and subscriber for packet.
pub fn packet_stream<T: Transport>() -> (
  PacketProducer<T::ResolvedAddress, <T::Runtime as RuntimeLite>::Instant>,
  PacketSubscriber<T::ResolvedAddress, <T::Runtime as RuntimeLite>::Instant>,
) {
  let (sender, receiver) = async_channel::unbounded();
  (PacketProducer { sender }, PacketSubscriber { receiver })
}

impl<A, T> PacketProducer<A, T> {
  /// Sends a packet into the producer.
  ///
  /// If the producer is full, this method waits until there is space for a message.
  ///
  /// If the producer is closed, this method returns an error.
  pub async fn send(&self, packet: Packet<A, T>) -> Result<(), SendError<Packet<A, T>>> {
    self.sender.send(packet).await
  }

  /// Attempts to send a packet into the producer.
  ///
  /// If the channel is full or closed, this method returns an error.
  pub fn try_send(&self, packet: Packet<A, T>) -> Result<(), TrySendError<Packet<A, T>>> {
    self.sender.try_send(packet)
  }

  /// Returns `true` if the producer is empty.
  pub fn is_empty(&self) -> bool {
    self.sender.is_empty()
  }

  /// Returns the number of packets in the producer.
  pub fn len(&self) -> usize {
    self.sender.len()
  }

  /// Returns `true` if the producer is full.
  pub fn is_full(&self) -> bool {
    self.sender.is_full()
  }

  /// Returns `true` if the producer is closed.
  pub fn is_closed(&self) -> bool {
    self.sender.is_closed()
  }

  /// Closes the producer. Returns `true` if the producer was closed by this call.
  pub fn close(&self) -> bool {
    self.sender.close()
  }
}

impl<A, T> PacketSubscriber<A, T> {
  /// Receives a packet from the subscriber.
  ///
  /// If the subscriber is empty, this method waits until there is a message.
  ///
  /// If the subscriber is closed, this method receives a message or returns an error if there are
  /// no more messages.
  pub async fn recv(&self) -> Result<Packet<A, T>, RecvError> {
    self.receiver.recv().await
  }

  /// Attempts to receive a message from the subscriber.
  ///
  /// If the subscriber is empty, or empty and closed, this method returns an error.
  pub fn try_recv(&self) -> Result<Packet<A, T>, TryRecvError> {
    self.receiver.try_recv()
  }

  /// Returns `true` if the subscriber is empty.
  pub fn is_empty(&self) -> bool {
    self.receiver.is_empty()
  }

  /// Returns the number of packets in the subscriber.
  pub fn len(&self) -> usize {
    self.receiver.len()
  }

  /// Returns `true` if the subscriber is full.
  pub fn is_full(&self) -> bool {
    self.receiver.is_full()
  }

  /// Returns `true` if the subscriber is closed.
  pub fn is_closed(&self) -> bool {
    self.receiver.is_closed()
  }

  /// Closes the subscriber. Returns `true` if the subscriber was closed by this call.
  pub fn close(&self) -> bool {
    self.receiver.close()
  }
}

impl<A, T> Stream for PacketSubscriber<A, T> {
  type Item = <Receiver<Packet<A, T>> as Stream>::Item;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <Receiver<_> as Stream>::poll_next(self.project().receiver, cx)
  }
}

/// Returns producer and subscriber for promised stream.
pub fn promised_stream<T: Transport>() -> (
  StreamProducer<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
  StreamSubscriber<<T::Resolver as AddressResolver>::ResolvedAddress, T::Stream>,
) {
  let (sender, receiver) = async_channel::bounded(1);
  (StreamProducer { sender }, StreamSubscriber { receiver })
}

/// A producer for promised streams.
#[derive(Debug)]
#[repr(transparent)]
pub struct StreamProducer<A, S> {
  sender: Sender<(A, S)>,
}

impl<A, S> Clone for StreamProducer<A, S> {
  fn clone(&self) -> Self {
    Self {
      sender: self.sender.clone(),
    }
  }
}

/// A subscriber for promised streams.
#[pin_project::pin_project]
#[derive(Debug)]
#[repr(transparent)]
pub struct StreamSubscriber<A, S> {
  #[pin]
  receiver: Receiver<(A, S)>,
}

impl<A, S> Clone for StreamSubscriber<A, S> {
  fn clone(&self) -> Self {
    Self {
      receiver: self.receiver.clone(),
    }
  }
}

impl<A, S> StreamProducer<A, S> {
  /// Sends a promised stream into the producer.
  ///
  /// If the producer is full, this method waits until there is space for a stream.
  ///
  /// If the producer is closed, this method returns an error.
  pub async fn send(&self, addr: A, conn: S) -> Result<(), SendError<(A, S)>> {
    self.sender.send((addr, conn)).await
  }

  /// Attempts to send a promised stream into the producer.
  ///
  /// If the producer is full or closed, this method returns an error.
  pub fn try_send(&self, addr: A, conn: S) -> Result<(), TrySendError<(A, S)>> {
    self.sender.try_send((addr, conn))
  }

  /// Returns `true` if the producer is empty.
  pub fn is_empty(&self) -> bool {
    self.sender.is_empty()
  }

  /// Returns the number of promised streams in the producer.
  pub fn len(&self) -> usize {
    self.sender.len()
  }

  /// Returns `true` if the producer is full.
  pub fn is_full(&self) -> bool {
    self.sender.is_full()
  }

  /// Returns `true` if the producer is closed.
  pub fn is_closed(&self) -> bool {
    self.sender.is_closed()
  }

  /// Closes the producer. Returns `true` if the producer was closed by this call.
  pub fn close(&self) -> bool {
    self.sender.close()
  }
}

impl<A, S> StreamSubscriber<A, S> {
  /// Receives a promised stream from the subscriber.
  ///
  /// If the subscriber is empty, this method waits until there is a message.
  ///
  /// If the subscriber is closed, this method receives a message or returns an error if there are
  /// no more messages.
  pub async fn recv(&self) -> Result<(A, S), RecvError> {
    self.receiver.recv().await
  }

  /// Attempts to receive a message from the channel.
  ///
  /// If the channel is empty, or empty and closed, this method returns an error.
  pub fn try_recv(&self) -> Result<(A, S), TryRecvError> {
    self.receiver.try_recv()
  }

  /// Returns `true` if the subscriber is empty.
  pub fn is_empty(&self) -> bool {
    self.receiver.is_empty()
  }

  /// Returns the number of promised streams in the subscriber.
  pub fn len(&self) -> usize {
    self.receiver.len()
  }

  /// Returns `true` if the subscriber is full.
  pub fn is_full(&self) -> bool {
    self.receiver.is_full()
  }

  /// Returns `true` if the subscriber is closed.
  pub fn is_closed(&self) -> bool {
    self.receiver.is_closed()
  }

  /// Closes the subscriber. Returns `true` if the subscriber was closed by this call.
  pub fn close(&self) -> bool {
    self.receiver.close()
  }
}

impl<A, S> Stream for StreamSubscriber<A, S> {
  type Item = <Receiver<(A, S)> as Stream>::Item;

  fn poll_next(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <Receiver<_> as Stream>::poll_next(self.project().receiver, cx)
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::*;

  async fn access<R: RuntimeLite>() {
    let messages = Message::<SmolStr, SocketAddr>::user_data(Bytes::new());
    let timestamp = R::now();
    let mut packet = Packet::<SocketAddr, _>::new(
      "127.0.0.1:8080".parse().unwrap(),
      timestamp,
      messages.encode_to_vec().unwrap(),
    );
    packet.set_from("127.0.0.1:8081".parse().unwrap());

    let start = R::now();
    packet.set_timestamp(start);
    let messages = Message::<SmolStr, SocketAddr>::user_data(Bytes::from_static(b"a"))
      .encode_to_vec()
      .unwrap();
    packet.set_payload(messages);
    assert_eq!(
      packet.payload(),
      &Message::<SmolStr, SocketAddr>::user_data(Bytes::from_static(b"a"))
        .encode_to_bytes()
        .unwrap(),
    );
    assert_eq!(
      *packet.from(),
      "127.0.0.1:8081".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(*packet.timestamp(), start);
  }

  #[test]
  fn tokio_access() {
    tokio::runtime::Builder::new_current_thread()
      .worker_threads(1)
      .enable_all()
      .build()
      .unwrap()
      .block_on(access::<agnostic_lite::tokio::TokioRuntime>());
  }
}
