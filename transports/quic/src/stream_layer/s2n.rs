use std::{io, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use futures::{AsyncReadExt, AsyncWriteExt, Future};
use s2n_quic::{
  stream::{BidirectionalStream, ReceiveStream, SendStream},
  Client, Server,
};

use super::{Listener, QuicBiStream, QuicReadStream, QuicWriteStream, StreamLayer};

mod error;
pub use error::*;
mod options;
pub use options::*;

/// A trait which is used to create a s2n [`Server`].
pub trait ServerCreator: Send + Sync + 'static {
  /// Build a server
  fn build(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Server>> + Send;
}

/// A trait which is used to create a s2n [`Client`].
pub trait ClientCreator: Send + Sync + 'static {
  /// Build a client
  fn build(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Client>> + Send;
}

/// A QUIC stream layer based on [`s2n`](::s2n_quic).
pub struct S2n<SC, CC> {
  server_creator: Arc<SC>,
  client_creator: Arc<CC>,
}

impl<SC, CC> StreamLayer for S2n<SC, CC>
where
  SC: ServerCreator,
  CC: ClientCreator,
{
  type Error = S2nError;

  type Listener = S2nListener;

  type Stream = S2nBiStream;

  fn max_stream_data(&self) -> usize {
    todo!()
  }

  async fn bind(&self, addr: SocketAddr) -> io::Result<Self::Listener> {
    let srv = self.server_creator.build(addr).await?;
    let client = self.client_creator.build(addr).await?;
    Ok(S2nListener {
      server: srv,
      client,
    })
  }
}

/// [`S2nListener`] is an implementation of [`Listener`] based on [`s2n_quic`].
pub struct S2nListener {
  server: Server,
  client: Client,
}

impl Listener for S2nListener {
  type Error = S2nError;
  type BiStream = S2nBiStream;

  type ReadStream = S2nReadStream;

  type WriteStream = S2nWriteStream;

  async fn accept_bi(&mut self) -> Result<Self::BiStream, Self::Error> {
    self
      .server
      .accept()
      .await
      .ok_or(Self::Error::Closed)?
      .accept_bidirectional_stream()
      .await
      .map_err(Into::into)
      .and_then(|conn| conn.map(S2nBiStream).ok_or(Self::Error::Closed))
  }

  async fn open_bi(&self, addr: SocketAddr) -> Result<Self::BiStream, Self::Error> {
    self
      .client
      .connect(addr.into())
      .await?
      .open_bidirectional_stream()
      .await
      .map(S2nBiStream)
      .map_err(Into::into)
  }

  async fn accept_uni(&mut self) -> Result<Self::ReadStream, Self::Error> {
    self
      .server
      .accept()
      .await
      .ok_or(Self::Error::Closed)?
      .accept_receive_stream()
      .await
      .map_err(Into::into)
      .and_then(|conn| conn.map(S2nReadStream).ok_or(Self::Error::Closed))
  }

  async fn open_uni(&self, addr: SocketAddr) -> Result<Self::WriteStream, Self::Error> {
    self
      .client
      .connect(addr.into())
      .await?
      .open_send_stream()
      .await
      .map(S2nWriteStream)
      .map_err(Into::into)
  }
}

/// [`S2nBiStream`] is an implementation of [`QuicBiStream`] based on [`s2n_quic`].
pub struct S2nBiStream(BidirectionalStream);

impl QuicBiStream for S2nBiStream {
  type Error = S2nError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let len = src.len();
    self.0.write_all(&src).await?;
    self.0.flush().await.map(|_| len).map_err(Into::into)
  }

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self.0.read(buf).await.map_err(Into::into)
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.read_exact(buf).await.map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.0.close().await.map_err(Into::into)
  }
}

/// [`S2nReadStream`] is an implementation of [`QuicReadStream`] based on [`s2n_quic`].
pub struct S2nReadStream(ReceiveStream);

impl QuicReadStream for S2nReadStream {
  type Error = S2nError;

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self.0.read(buf).await.map_err(Into::into)
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.read_exact(buf).await.map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    Ok(())
  }
}

/// [`S2nWriteStream`] is an implementation of [`QuicWriteStream`] based on [`s2n_quic`].
pub struct S2nWriteStream(SendStream);

impl QuicWriteStream for S2nWriteStream {
  type Error = S2nError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let len = src.len();
    self.0.write_all(&src).await?;
    self.0.flush().await.map(|_| len).map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.0.close().await.map_err(Into::into)
  }
}
