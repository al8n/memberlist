#![allow(warnings)]

use std::{
  io,
  marker::PhantomData,
  net::SocketAddr,
  path::PathBuf,
  pin::Pin,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  task::{Context, Poll},
  time::Duration,
};

use agnostic_lite::{time::Instant, RuntimeLite};
use bytes::Bytes;
use futures::{lock::Mutex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
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

impl<R: RuntimeLite> StreamLayer for S2n<R> {
  type Runtime = R;
  type Acceptor = S2nBiAcceptor;
  type Connector = S2nConnector;
  type Connection = S2nConnection;
  type Stream = S2nStream;
  type Options = options::Options;

  fn max_stream_data(&self) -> usize {
    self.max_stream_data
  }

  async fn new(opts: Self::Options) -> io::Result<Self> {
    Ok(Self {
      limits: Limits::try_from(&opts)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
      server_name: opts.server_name,
      max_stream_data: opts.data_window as usize,
      max_remote_open_streams: opts.max_open_remote_bidirectional_streams as usize,
      max_local_open_streams: opts.max_open_local_bidirectional_streams as usize,
      cert: opts.cert_path,
      key: opts.key_path,
      _marker: PhantomData,
    })
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
    };

    let connector = Self::Connector {
      client,
      server_name: self.server_name.clone(),
      local_addr: actual_client_addr,
      max_open_streams: self.max_remote_open_streams,
    };
    Ok((actual_local_addr, acceptor, connector))
  }
}

/// [`S2nBiAcceptor`] is an implementation of [`QuicBiAcceptor`] based on [`s2n_quic`].
pub struct S2nBiAcceptor {
  server: Server,
  local_addr: SocketAddr,
  max_open_streams: usize,
}

impl QuicAcceptor for S2nBiAcceptor {
  type Connection = S2nConnection;

  async fn accept(&mut self) -> io::Result<(Self::Connection, SocketAddr)> {
    let mut conn = self
      .server
      .accept()
      .await
      .ok_or(io::Error::new(io::ErrorKind::Other, "server closed"))?;
    conn.keep_alive(true)?;
    let remote = conn.remote_addr()?;
    Ok((
      S2nConnection::new(conn, self.max_open_streams, self.local_addr, remote),
      remote,
    ))
  }

  async fn close(&mut self) -> io::Result<()> {
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nConnector`] is an implementation of [`QuicConnector`] based on [`s2n_quic`].
pub struct S2nConnector {
  client: Client,
  local_addr: SocketAddr,
  server_name: SmolStr,
  max_open_streams: usize,
}

impl QuicConnector for S2nConnector {
  type Connection = S2nConnection;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Connection> {
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

  async fn close(&self) -> io::Result<()> {
    // TODO: figure out how to close a client
    Ok(())
  }

  async fn wait_idle(&self) -> io::Result<()> {
    // self.client.wait_idle().await.map_err(Into::into)
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`S2nConnection`] is an implementation of [`QuicConnection`] based on [`s2n_quic`].
pub struct S2nConnection {
  connection: Arc<Mutex<Connection>>,
  current_open_streams: Arc<AtomicUsize>,
  max_open_streams: usize,
  local_addr: SocketAddr,
  remote_addr: SocketAddr,
}

impl S2nConnection {
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
    }
  }
}

impl QuicConnection for S2nConnection {
  type Stream = S2nStream;

  async fn accept_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
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
              S2nStream::new(conn, self.current_open_streams.clone()),
              self.remote_addr,
            )
          })
          .ok_or(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "connection closed",
          ))
      })
  }

  async fn open_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    self
      .connection
      .lock()
      .await
      .open_bidirectional_stream()
      .await
      .map(|conn| {
        self.current_open_streams.fetch_add(1, Ordering::AcqRel);
        (
          S2nStream::new(conn, self.current_open_streams.clone()),
          self.remote_addr,
        )
      })
      .map_err(Into::into)
  }

  async fn close(&self) -> io::Result<()> {
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
pub struct S2nStream {
  recv_stream: AsyncPeekable<ReceiveStream>,
  send_stream: SendStream,
  ctr: Arc<AtomicUsize>,
}

impl S2nStream {
  fn new(stream: BidirectionalStream, ctr: Arc<AtomicUsize>) -> Self {
    let (recv, send) = stream.split();
    Self {
      recv_stream: recv.peekable(),
      send_stream: send,
      ctr,
    }
  }
}

impl Drop for S2nStream {
  fn drop(&mut self) {
    self.ctr.fetch_sub(1, Ordering::AcqRel);
  }
}

impl QuicStream for S2nStream {
  async fn read_packet(&mut self) -> std::io::Result<Bytes> {
    todo!()
  }
}

impl AsyncRead for S2nStream {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut [u8],
  ) -> Poll<std::io::Result<usize>> {
    let recv = Pin::new(&mut self.recv_stream);
    recv.poll_read(cx, buf)
  }
}

impl AsyncWrite for S2nStream {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    let send = Pin::new(&mut self.send_stream);
    send.poll_write(cx, buf)
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let send = Pin::new(&mut self.send_stream);
    send.poll_flush(cx)
  }

  fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let send = Pin::new(&mut self.send_stream);
    send.poll_close(cx)
  }
}

fn invalid_data<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
