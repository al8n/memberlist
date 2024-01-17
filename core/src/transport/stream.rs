use async_channel::{Receiver, Sender};
pub use async_channel::{RecvError, SendError, TryRecvError, TrySendError};
use futures::Stream;

use super::*;

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketProducer {
  sender: Sender<Packet>,
}

#[pin_project::pin_project]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketSubscriber {
  #[pin]
  receiver: Receiver<Packet>,
}

pub fn packet_stream() -> (PacketProducer, PacketSubscriber) {
  let (sender, receiver) = async_channel::unbounded();
  (PacketProducer { sender }, PacketSubscriber { receiver })
}

impl PacketProducer {
  /// Sends a packet into the producer.
  ///
  /// If the producer is full, this method waits until there is space for a message.
  ///
  /// If the producer is closed, this method returns an error.
  pub async fn send(&self, packet: Packet) -> Result<(), SendError<Packet>> {
    self.sender.send(packet).await
  }

  /// Attempts to send a packet into the producer.
  ///
  /// If the channel is full or closed, this method returns an error.
  pub fn try_send(&self, packet: Packet) -> Result<(), TrySendError<Packet>> {
    self.sender.try_send(packet)
  }
}

impl PacketSubscriber {
  /// Receives a packet from the subscriber.
  ///
  /// If the subscriber is empty, this method waits until there is a message.
  ///
  /// If the subscriber is closed, this method receives a message or returns an error if there are
  /// no more messages.
  pub async fn recv(&self) -> Result<Packet, RecvError> {
    self.receiver.recv().await
  }

  /// Attempts to receive a message from the subscriber.
  ///
  /// If the subscriber is empty, or empty and closed, this method returns an error.
  pub fn try_recv(&self) -> Result<Packet, TryRecvError> {
    self.receiver.try_recv()
  }
}

impl Stream for PacketSubscriber {
  type Item = <Receiver<Packet> as Stream>::Item;

  fn poll_next(
    mut self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <Receiver<_> as Stream>::poll_next(self.project().receiver, cx)
  }
}

pub fn promised_stream<T: Transport>() -> (
  StreamProducer<T::PromisedStream>,
  StreamSubscriber<T::PromisedStream>,
) {
  let (sender, receiver) = async_channel::bounded(1);
  (StreamProducer { sender }, StreamSubscriber { receiver })
}

#[derive(Debug)]
#[repr(transparent)]
pub struct StreamProducer<S> {
  sender: Sender<S>,
}

impl<S> Clone for StreamProducer<S> {
  fn clone(&self) -> Self {
    Self {
      sender: self.sender.clone(),
    }
  }
}

#[pin_project::pin_project]
#[derive(Debug)]
#[repr(transparent)]
pub struct StreamSubscriber<S> {
  #[pin]
  receiver: Receiver<S>,
}

impl<S> Clone for StreamSubscriber<S> {
  fn clone(&self) -> Self {
    Self {
      receiver: self.receiver.clone(),
    }
  }
}

impl<S> StreamProducer<S> {
  /// Sends a promised stream into the producer.
  ///
  /// If the producer is full, this method waits until there is space for a stream.
  ///
  /// If the producer is closed, this method returns an error.
  pub async fn send(&self, conn: S) -> Result<(), SendError<S>> {
    self.sender.send(conn).await
  }

  /// Attempts to send a promised stream into the producer.
  ///
  /// If the producer is full or closed, this method returns an error.
  pub fn try_send(&self, conn: S) -> Result<(), TrySendError<S>> {
    self.sender.try_send(conn)
  }
}

impl<S> StreamSubscriber<S> {
  /// Receives a promised stream from the subscriber.
  ///
  /// If the subscriber is empty, this method waits until there is a message.
  ///
  /// If the subscriber is closed, this method receives a message or returns an error if there are
  /// no more messages.
  pub async fn recv(&self) -> Result<S, RecvError> {
    self.receiver.recv().await
  }

  /// Attempts to receive a message from the channel.
  ///
  /// If the channel is empty, or empty and closed, this method returns an error.
  pub fn try_recv(&self) -> Result<S, TryRecvError> {
    self.receiver.try_recv()
  }
}

impl<S> Stream for StreamSubscriber<S> {
  type Item = <Receiver<S> as Stream>::Item;

  fn poll_next(
    mut self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <Receiver<_> as Stream>::poll_next(self.project().receiver, cx)
  }
}
