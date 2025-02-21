#![allow(warnings)]

use std::{
  cmp, io,
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
use memberlist_core::proto::{MediumVec, SmallVec};
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
    Ok((S2nConnection::new(conn, self.local_addr, remote), remote))
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
}

impl QuicConnector for S2nConnector {
  type Connection = S2nConnection;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Connection> {
    let connect = Connect::new(addr).with_server_name(self.server_name.as_str());
    let mut conn = self.client.connect(connect).await?;
    conn.keep_alive(true)?;
    let remote = conn.remote_addr()?;
    Ok(S2nConnection::new(conn, self.local_addr, remote))
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
  local_addr: SocketAddr,
  remote_addr: SocketAddr,
}

impl S2nConnection {
  #[inline]
  fn new(connection: Connection, local_addr: SocketAddr, remote_addr: SocketAddr) -> Self {
    Self {
      connection: Arc::new(Mutex::new(connection)),
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
        conn
          .map(|conn| (S2nStream::new(conn), self.remote_addr))
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
      .map(|conn| (S2nStream::new(conn), self.remote_addr))
      .map_err(Into::into)
  }

  async fn open_uni(&self) -> io::Result<(<Self::Stream as QuicStream>::SendStream, SocketAddr)> {
    self
      .connection
      .lock()
      .await
      .open_send_stream()
      .await
      .map(|conn| (conn, self.remote_addr))
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

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// A [`ProtoReader`](memberlist_core::proto::ProtoReader) implementation for S2n stream layer
pub struct S2nProtoReader {
  stream: AsyncPeekable<ReceiveStream>,
}

impl From<ReceiveStream> for S2nProtoReader {
  fn from(stream: ReceiveStream) -> Self {
    Self {
      stream: stream.peekable(),
    }
  }
}

impl memberlist_core::proto::ProtoReader for S2nProtoReader {
  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.stream.peek(buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.stream.peek_exact(buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    self.stream.read(buf).await
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    self.stream.read_exact(buf).await
  }
}

/// [`S2nStream`] is an implementation of [`QuicBiStream`] based on [`s2n_quic`].
pub struct S2nStream {
  recv_stream: S2nProtoReader,
  send_stream: SendStream,
}

impl S2nStream {
  fn new(stream: BidirectionalStream) -> Self {
    let (recv, send) = stream.split();
    Self {
      recv_stream: recv.into(),
      send_stream: send,
    }
  }
}

impl memberlist_core::transport::Connection for S2nStream {
  type Reader = S2nProtoReader;
  type Writer = SendStream;

  fn split(self) -> (Self::Reader, Self::Writer) {
    (self.recv_stream, self.send_stream)
  }

  async fn close(&mut self) -> std::io::Result<()> {
    self.send_stream.close().await.map_err(Into::into)
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    self.send_stream.write_all(payload).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    self.send_stream.flush().await.map_err(Into::into)
  }

  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    memberlist_core::proto::ProtoReader::peek(&mut self.recv_stream, buf).await
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    memberlist_core::proto::ProtoReader::read_exact(&mut self.recv_stream, buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    memberlist_core::proto::ProtoReader::read(&mut self.recv_stream, buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    memberlist_core::proto::ProtoReader::peek_exact(&mut self.recv_stream, buf).await
  }
}

impl QuicStream for S2nStream {
  type SendStream = SendStream;

  async fn read_packet(&mut self) -> std::io::Result<Bytes> {
    let (peeked, recv) = self.recv_stream.stream.get_mut();
    let mut chunks = SmallVec::with_capacity(4);
    chunks.fill_with(Bytes::new);

    let mut data = peeked.to_vec();
    loop {
      match recv.receive_vectored(&mut chunks).await {
        Ok((count, open)) => {
          if !open {
            for chunk in chunks.iter().take(count) {
              data.extend_from_slice(chunk);
            }
            return Ok(Bytes::from(data));
          }

          for chunk in chunks.iter().take(count) {
            data.extend_from_slice(chunk);
          }
        }
        Err(e) => return Err(e.into()),
      }
    }
  }
}

fn invalid_data<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
