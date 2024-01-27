use std::{marker::PhantomData, net::SocketAddr, sync::Arc};

use agnostic::{net::Net, Runtime};
use bytes::Bytes;
use quinn::{ClientConfig, ConnectError, Endpoint, RecvStream, SendStream, VarInt};
use smol_str::SmolStr;

mod error;
pub use error::*;

mod options;
pub use options::*;

use super::{Listener, QuicBiStream, QuicReadStream, QuicWriteStream, StreamLayer};

/// [`Quinn`] is an implementation of [`StreamLayer`] based on [`quinn`].
pub struct Quinn<R> {
  opts: QuinnOptions,
  _marker: PhantomData<R>,
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Error = QuinnError;
  type Listener = QuinnListener;
  type Stream = QuinnBiStream;

  fn max_stream_data(&self) -> usize {
    self.opts.max_stream_data.min(self.opts.max_connection_data)
  }

  async fn bind(&self, addr: SocketAddr) -> std::io::Result<Self::Listener> {
    let server_name = self.opts.server_name.clone();

    let client_config = self.opts.client_config.clone();
    let sock = socket2::Socket::new(
      socket2::Domain::for_address(addr),
      socket2::Type::DGRAM,
      Some(socket2::Protocol::UDP),
    )?;
    sock.set_nonblocking(true)?;

    let endpoint = Endpoint::new(
      self.opts.endpoint_config.clone(),
      Some(self.opts.server_config.clone()),
      sock.into(),
      Arc::new(<R::Net as Net>::Quinn::default()),
    )?;

    Ok(QuinnListener {
      server_name,
      endpoint,
      client_config,
    })
  }
}

/// [`QuinnListener`] is an implementation of [`Listener`] based on [`quinn`].
pub struct QuinnListener {
  server_name: SmolStr,
  endpoint: Endpoint,
  client_config: ClientConfig,
}

impl Listener for QuinnListener {
  type Error = QuinnError;
  type BiStream = QuinnBiStream;
  type ReadStream = QuinnReadStream;
  type WriteStream = QuinnWriteStream;

  async fn accept_bi(&mut self) -> Result<Self::BiStream, Self::Error> {
    self
      .endpoint
      .accept()
      .await
      .ok_or(ConnectError::EndpointStopping)?
      .await?
      .accept_bi()
      .await
      .map(|(send, recv)| QuinnBiStream { send, recv })
      .map_err(Into::into)
  }

  async fn open_bi(&self, addr: SocketAddr) -> Result<Self::BiStream, Self::Error> {
    self
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)?
      .await?
      .open_bi()
      .await
      .map(|(send, recv)| QuinnBiStream { send, recv })
      .map_err(Into::into)
  }

  async fn accept_uni(&mut self) -> Result<Self::ReadStream, Self::Error> {
    self
      .endpoint
      .accept()
      .await
      .ok_or(ConnectError::EndpointStopping)?
      .await?
      .accept_uni()
      .await
      .map(QuinnReadStream)
      .map_err(Into::into)
  }

  async fn open_uni(&self, addr: SocketAddr) -> Result<Self::WriteStream, Self::Error> {
    self
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)?
      .await?
      .open_uni()
      .await
      .map(QuinnWriteStream)
      .map_err(Into::into)
  }
}

/// [`QuinnReadStream`] is an implementation of [`QuicReadStream`] based on [`quinn`].
pub struct QuinnReadStream(RecvStream);

impl QuicReadStream for QuinnReadStream {
  type Error = QuinnReadStreamError;

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self
      .0
      .read(buf)
      .await
      .map(|read| read.unwrap_or(0))
      .map_err(Into::into)
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.0.read_exact(buf).await.map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self
      .0
      .stop(VarInt::from_u32(0))
      .map_err(|_| QuinnReadStreamError::Read(quinn::ReadError::UnknownStream))
  }
}

/// [`QuinnWriteStream`] is an implementation of [`QuicWriteStream`] based on [`quinn`].
pub struct QuinnWriteStream(SendStream);

impl QuicWriteStream for QuinnWriteStream {
  type Error = QuinnWriteStreamError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let sent = src.len();
    self
      .0
      .write_all(&src)
      .await
      .map(|_| sent)
      .map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.0.finish().await.map_err(Into::into)
  }
}

/// [`QuinnBiStream`] is an implementation of [`QuicBiStream`] based on [`quinn`].
pub struct QuinnBiStream {
  send: SendStream,
  recv: RecvStream,
}

impl QuicBiStream for QuinnBiStream {
  type Error = QuinnBiStreamError;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let sent = src.len();
    self
      .send
      .write_all(&src)
      .await
      .map(|_| sent)
      .map_err(Into::into)
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    self.recv.read_exact(buf).await.map_err(Into::into)
  }

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    self
      .recv
      .read(buf)
      .await
      .map(|read| read.unwrap_or(0))
      .map_err(Into::into)
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.send.finish().await.map_err(QuinnBiStreamError::from)?;
    self.recv.stop(VarInt::from_u32(0)).map_err(|_| {
      QuinnBiStreamError::Read(QuinnReadStreamError::Read(quinn::ReadError::UnknownStream))
    })
  }
}
