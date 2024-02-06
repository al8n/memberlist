use async_channel::{Receiver, Sender};
pub use async_channel::{RecvError, SendError, TryRecvError, TrySendError};
use futures::Stream;

use super::*;

/// A producer for packets.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketProducer<I, A> {
  sender: Sender<Packet<I, A>>,
}

/// A subscriber for packets.
#[pin_project::pin_project]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketSubscriber<I, A> {
  #[pin]
  receiver: Receiver<Packet<I, A>>,
}

/// Returns producer and subscriber for packet.
pub fn packet_stream<T: Transport>() -> (
  PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  PacketSubscriber<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) {
  let (sender, receiver) = async_channel::unbounded();
  (PacketProducer { sender }, PacketSubscriber { receiver })
}

impl<I, A> PacketProducer<I, A> {
  /// Sends a packet into the producer.
  ///
  /// If the producer is full, this method waits until there is space for a message.
  ///
  /// If the producer is closed, this method returns an error.
  pub async fn send(&self, packet: Packet<I, A>) -> Result<(), SendError<Packet<I, A>>> {
    self.sender.send(packet).await
  }

  /// Attempts to send a packet into the producer.
  ///
  /// If the channel is full or closed, this method returns an error.
  pub fn try_send(&self, packet: Packet<I, A>) -> Result<(), TrySendError<Packet<I, A>>> {
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

impl<I, A> PacketSubscriber<I, A> {
  /// Receives a packet from the subscriber.
  ///
  /// If the subscriber is empty, this method waits until there is a message.
  ///
  /// If the subscriber is closed, this method receives a message or returns an error if there are
  /// no more messages.
  pub async fn recv(&self) -> Result<Packet<I, A>, RecvError> {
    self.receiver.recv().await
  }

  /// Attempts to receive a message from the subscriber.
  ///
  /// If the subscriber is empty, or empty and closed, this method returns an error.
  pub fn try_recv(&self) -> Result<Packet<I, A>, TryRecvError> {
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

impl<I, A> Stream for PacketSubscriber<I, A> {
  type Item = <Receiver<Packet<I, A>> as Stream>::Item;

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
