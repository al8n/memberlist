use async_channel::{Receiver, RecvError, SendError, Sender};
use futures::Stream;

use super::*;

pub struct PacketProducerError(SendError<Packet>);

impl core::fmt::Debug for PacketProducerError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl core::fmt::Display for PacketProducerError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for PacketProducerError {}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketProducer {
  sender: Sender<Packet>,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct PacketSubscriber {
  receiver: Receiver<Packet>,
}

pub struct PacketSubscriberError(RecvError);

pub fn packet_stream() -> (PacketProducer, PacketSubscriber) {
  let (sender, receiver) = async_channel::unbounded();
  (PacketProducer { sender }, PacketSubscriber { receiver })
}

impl PacketProducer {
  pub async fn send(&self, packet: Packet) -> Result<(), PacketProducerError> {
    self.sender.send(packet).await.map_err(PacketProducerError)
  }
}

impl PacketSubscriber {
  pub async fn recv(&self) -> Result<Packet, PacketSubscriberError> {
    self.receiver.recv().await.map_err(PacketSubscriberError)
  }
}

impl Stream for PacketSubscriber {
  type Item = <Receiver<Packet> as Stream>::Item;

  fn poll_next(
    mut self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <Receiver<_> as Stream>::poll_next(std::pin::Pin::new(&mut self.receiver), cx)
  }
}

impl core::fmt::Debug for PacketSubscriberError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl core::fmt::Display for PacketSubscriberError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for PacketSubscriberError {}

pub struct ConnectionProducerError<T: Transport>(SendError<ReliableConnection<T>>);

impl<T: Transport> core::fmt::Debug for ConnectionProducerError<T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl<T: Transport> core::fmt::Display for ConnectionProducerError<T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<T: Transport> std::error::Error for ConnectionProducerError<T> {}

#[derive(Debug)]
#[repr(transparent)]
pub struct ConnectionProducer<T: Transport> {
  sender: Sender<ReliableConnection<T>>,
}

impl<T: Transport> Clone for ConnectionProducer<T> {
  fn clone(&self) -> Self {
    Self {
      sender: self.sender.clone(),
    }
  }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct ConnectionSubscriber<T: Transport> {
  receiver: Receiver<ReliableConnection<T>>,
}

impl<T: Transport> Clone for ConnectionSubscriber<T> {
  fn clone(&self) -> Self {
    Self {
      receiver: self.receiver.clone(),
    }
  }
}

pub struct ConnectionSubscriberError(RecvError);

impl core::fmt::Debug for ConnectionSubscriberError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl core::fmt::Display for ConnectionSubscriberError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for ConnectionSubscriberError {}

pub fn connection_stream<T: Transport>() -> (ConnectionProducer<T>, ConnectionSubscriber<T>) {
  let (sender, receiver) = async_channel::bounded(1);
  (
    ConnectionProducer { sender },
    ConnectionSubscriber { receiver },
  )
}

impl<T: Transport> ConnectionProducer<T> {
  pub async fn send(&self, conn: ReliableConnection<T>) -> Result<(), ConnectionProducerError<T>> {
    self
      .sender
      .send(conn)
      .await
      .map_err(ConnectionProducerError)
  }
}

impl<T: Transport> ConnectionSubscriber<T> {
  pub async fn recv(&self) -> Result<ReliableConnection<T>, ConnectionSubscriberError> {
    self
      .receiver
      .recv()
      .await
      .map_err(ConnectionSubscriberError)
  }
}

impl<T: Transport> Stream for ConnectionSubscriber<T> {
  type Item = <Receiver<ReliableConnection<T>> as Stream>::Item;

  fn poll_next(
    mut self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<Option<Self::Item>> {
    <Receiver<_> as Stream>::poll_next(std::pin::Pin::new(&mut self.receiver), cx)
  }
}
