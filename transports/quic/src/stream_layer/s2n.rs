#![allow(warnings)]

use std::{
  io,
  marker::PhantomData,
  net::SocketAddr,
  path::PathBuf,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use agnostic_lite::{time::Instant, RuntimeLite};
use bytes::Bytes;
use futures::{lock::Mutex, AsyncReadExt, AsyncWriteExt};
use memberlist_core::transport::{TimeoutableReadStream, TimeoutableWriteStream};
use peekable::future::{AsyncPeekExt, AsyncPeekable};
use s2n_quic::{
  client::Connect,
  provider::limits::Limits,
  stream::{BidirectionalStream, ReceiveStream, SendStream},
  Client, Connection, Server,
};
use smol_str::SmolStr;

use crate::QuicConnection;

use super::{QuicAcceptor, QuicConnector, QuicStream, StreamLayer};

mod error;
pub use error::*;
mod options;
pub use options::*;

/// A QUIC stream layer based on [`s2n`](::s2n_quic).
pub struct S2n<R> {
  limits: Limits,
  server_name: SmolStr,
  max_stream_data: usize,
  max_remote_open_streams: usize,
  max_local_open_streams: usize,
  cert: PathBuf,
  key: PathBuf,
  _marker: PhantomData<R>,
}

impl<R> S2n<R> {
  /// Creates a new [`S2n`] stream layer with the given options.
  fn new_in(opts: Options) -> Result<Self, ValidationError> {
    Ok(Self {
      limits: Limits::try_from(&opts)?,
      server_name: opts.server_name,
      max_stream_data: opts.data_window as usize,
      max_remote_open_streams: opts.max_open_remote_bidirectional_streams as usize,
      max_local_open_streams: opts.max_open_local_bidirectional_streams as usize,
      cert: opts.cert_path,
      key: opts.key_path,
      _marker: PhantomData,
    })
  }
}

impl<R: RuntimeLite> StreamLayer for S2n<R> {
  type Runtime = R;
  type Error = S2nError;
  type Acceptor = S2nBiAcceptor<R>;
  type Connector = S2nConnector<R>;
  type Connection = S2nConnection<R>;
  type Stream = S2nStream<R>;
  type Options = options::Options;

  fn max_stream_data(&self) -> usize {
    self.max_stream_data
  }

  async fn new(opts: Self::Options) -> Result<Self, Self::Error> {
    Self::new_in(opts).map_err(Self::Error::Validation)
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> io::Result<(SocketAddr, Self::Acceptor, Self::Connector)> {
    let auto_server_port = addr.port() == 0;
    let srv = Server::builder()
      .with_limits(self.limits)
      .map_err(invalid_data)?
      .with_io(addr)
      .map_err(invalid_data)?
      .with_tls((self.cert.as_path(), self.key.as_path()))
      .map_err(invalid_data)?
      .start()
      .map_err(invalid_data)?;
    let mut addr = srv.local_addr()?;
    let client_addr: SocketAddr = match addr {
      SocketAddr::V4(_) => {
        addr.set_port(0);
        addr
      }
      SocketAddr::V6(_) => {
        addr.set_port(0);
        addr
      }
    };
    let client = Client::builder()
      .with_limits(self.limits)
      .map_err(invalid_data)?
      .with_tls(self.cert.as_path())
      .map_err(invalid_data)?
      .with_io(client_addr)?
      .start()
      .map_err(invalid_data)?;
    let actual_client_addr = client.local_addr()?;
    tracing::info!(
      "memberlist_quic: bind client to dynamic address {}",
      actual_client_addr
    );

    let actual_local_addr = srv.local_addr()?;
    if auto_server_port {
      tracing::info!(
        "memberlist_quic: bind server to dynamic address {}",
        actual_local_addr
      );
    }

    let acceptor = Self::Acceptor {
      server: srv,
      local_addr: actual_local_addr,
      max_open_streams: self.max_local_open_streams,
      _marker: PhantomData,
    };

    let connector = Self::Connector {
      client,
      server_name: self.server_name.clone(),
      local_addr: actual_client_addr,
      max_open_streams: self.max_remote_open_streams,
      _marker: PhantomData,
    };
    Ok((actual_local_addr, acceptor, connector))
  }
}

/// [`S2nBiAcceptor`] is an implementation of [`QuicBiAcceptor`] based on [`s2n_quic`].
pub struct S2nBiAcceptor<R> {
  server: Server,
  local_addr: SocketAddr,
  max_open_streams: usize,
  _marker: PhantomData<R>,
}

impl<R: RuntimeLite> QuicAcceptor for S2nBiAcceptor<R> {
  type Error = S2nError;
  type Connection = S2nConnection<R>;

  async fn accept(&mut self) -> Result<(Self::Connection, SocketAddr), Self::Error> {
    let mut conn = self.server.accept().await.ok_or(Self::Error::Closed)?;
    conn.keep_alive(true)?;
    let remote = conn.remote_addr()?;
    Ok((
      S2nConnection::new(conn, self.max_open_streams, self.local_addr, remote),
      remote,
    ))
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nConnector`] is an implementation of [`QuicConnector`] based on [`s2n_quic`].
pub struct S2nConnector<R> {
  client: Client,
  local_addr: SocketAddr,
  server_name: SmolStr,
  max_open_streams: usize,
  _marker: PhantomData<R>,
}

impl<R: RuntimeLite> QuicConnector for S2nConnector<R> {
  type Error = S2nError;
  type Connection = S2nConnection<R>;

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection, Self::Error> {
    let connect = Connect::new(addr).with_server_name(self.server_name.as_str());
    let mut conn = self.client.connect(connect).await?;
    conn.keep_alive(true)?;
    let remote = conn.remote_addr()?;
    Ok(S2nConnection::new(
      conn,
      self.max_open_streams,
      self.local_addr,
      remote,
    ))
  }

  async fn close(&self) -> Result<(), Self::Error> {
    // TODO: figure out how to close a client
    Ok(())
  }

  async fn wait_idle(&self) -> Result<(), Self::Error> {
    // self.client.wait_idle().await.map_err(Into::into)
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nConnection`] is an implementation of [`QuicConnection`] based on [`s2n_quic`].
pub struct S2nConnection<R> {
  connection: Arc<Mutex<Connection>>,
  current_open_streams: Arc<AtomicUsize>,
  max_open_streams: usize,
  local_addr: SocketAddr,
  remote_addr: SocketAddr,
  _marker: PhantomData<R>,
}

impl<R> S2nConnection<R> {
  #[inline]
  fn new(
    connection: Connection,
    max_open_streams: usize,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
  ) -> Self {
    Self {
      connection: Arc::new(Mutex::new(connection)),
      current_open_streams: Arc::new(AtomicUsize::new(0)),
      max_open_streams,
      local_addr,
      remote_addr,
      _marker: PhantomData,
    }
  }
}

impl<R: RuntimeLite> QuicConnection for S2nConnection<R> {
  type Error = S2nError;
  type Stream = S2nStream<R>;
  type Instant = R::Instant;

  async fn accept_bi(&self) -> Result<(Self::Stream, SocketAddr), Self::Error> {
    self
      .connection
      .lock()
      .await
      .accept_bidirectional_stream()
      .await
      .map_err(Into::into)
      .and_then(|conn| {
        self.current_open_streams.fetch_add(1, Ordering::AcqRel);
        conn
          .map(|conn| {
            (
              S2nStream::<R>::new(conn, self.current_open_streams.clone()),
              self.remote_addr,
            )
          })
          .ok_or(Self::Error::Closed)
      })
  }

  async fn open_bi(&self) -> Result<(Self::Stream, SocketAddr), Self::Error> {
    self
      .connection
      .lock()
      .await
      .open_bidirectional_stream()
      .await
      .map(|conn| {
        self.current_open_streams.fetch_add(1, Ordering::AcqRel);
        (
          S2nStream::<R>::new(conn, self.current_open_streams.clone()),
          self.remote_addr,
        )
      })
      .map_err(Into::into)
  }

  async fn open_bi_with_deadline(
    &self,
    deadline: R::Instant,
  ) -> Result<(Self::Stream, SocketAddr), Self::Error> {
    R::timeout_at(deadline, async { self.open_bi().await })
      .await
      .map_err(|_| Self::Error::Timeout)?
  }

  async fn close(&self) -> Result<(), Self::Error> {
    self.connection.lock().await.close(0u32.into());
    Ok(())
  }

  async fn is_closed(&self) -> bool {
    match self.connection.lock().await.ping() {
      Ok(_) => false,
      Err(e) => true,
    }
  }

  fn is_full(&self) -> bool {
    self.current_open_streams.load(Ordering::Acquire) >= self.max_open_streams
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nStream`] is an implementation of [`QuicBiStream`] based on [`s2n_quic`].
pub struct S2nStream<R: RuntimeLite> {
  recv_stream: AsyncPeekable<ReceiveStream>,
  send_stream: SendStream,
  read_deadline: Option<R::Instant>,
  write_deadline: Option<R::Instant>,
  ctr: Arc<AtomicUsize>,
  _marker: PhantomData<R>,
}

impl<R: RuntimeLite> S2nStream<R> {
  fn new(stream: BidirectionalStream, ctr: Arc<AtomicUsize>) -> Self {
    let (recv, send) = stream.split();
    Self {
      recv_stream: recv.peekable(),
      send_stream: send,
      read_deadline: None,
      write_deadline: None,
      ctr,
      _marker: PhantomData,
    }
  }
}

impl<R: RuntimeLite> Drop for S2nStream<R> {
  fn drop(&mut self) {
    self.ctr.fetch_sub(1, Ordering::AcqRel);
  }
}

impl<R: RuntimeLite> TimeoutableReadStream for S2nStream<R> {
  type Instant = R::Instant;

  fn set_read_deadline(&mut self, deadline: Option<Self::Instant>) {
    self.read_deadline = deadline;
  }

  fn read_deadline(&self) -> Option<Self::Instant> {
    self.read_deadline
  }
}

impl<R: RuntimeLite> TimeoutableWriteStream for S2nStream<R> {
  type Instant = R::Instant;

  fn set_write_deadline(&mut self, deadline: Option<Self::Instant>) {
    self.write_deadline = deadline;
  }

  fn write_deadline(&self) -> Option<Self::Instant> {
    self.write_deadline
  }
}

impl<R: RuntimeLite> QuicStream for S2nStream<R> {
  type Error = S2nError;
  type Instant = R::Instant;

  async fn write_all(&mut self, src: Bytes) -> Result<usize, Self::Error> {
    let len = src.len();
    let fut = async {
      self
        .send_stream
        .write_all(&src)
        .await
        .map(|_| len)
        .map_err(Into::into)
    };

    match self.write_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn flush(&mut self) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn finish(&mut self) -> Result<(), Self::Error> {
    let fut = async { self.send_stream.flush().await.map_err(Into::into) };

    match self.write_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
    let fut = async { self.recv_stream.read(buf).await.map_err(Into::into) };

    match self.read_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.recv_stream.read_exact(buf).await.map_err(Into::into) };

    match self.read_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
    let fut = async { self.recv_stream.peek_exact(buf).await.map_err(Into::into) };

    match self.read_deadline {
      Some(timeout) => R::timeout_at(timeout, fut)
        .await
        .map_err(|_| Self::Error::IO(io::Error::new(io::ErrorKind::TimedOut, "timeout")))?,
      None => fut.await.map_err(Into::into),
    }
  }

  async fn close(&mut self) -> Result<(), Self::Error> {
    self.send_stream.close().await.map_err(Into::into)
  }
}

impl<R: RuntimeLite> futures::AsyncRead for S2nStream<R> {
  fn poll_read(
    mut self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
    buf: &mut [u8],
  ) -> std::task::Poll<std::io::Result<usize>> {
    use futures::Future;

    let fut = self.recv_stream.read(buf);
    futures::pin_mut!(fut);
    fut.poll(cx)
  }
}

fn invalid_data<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
