use std::{cmp, io, marker::PhantomData, net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use agnostic::Runtime;
use futures::AsyncWriteExt;
use memberlist_core::proto::MediumVec;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, VarInt};
use smol_str::SmolStr;

mod options;
pub use options::*;

use super::{QuicAcceptor, QuicConnection, QuicConnector, QuicStream, StreamLayer};

/// Shared endpoint wrapper that closes the underlying `Endpoint` only
/// when the last holder drops it, preventing an acceptor from killing
/// a connector (or vice versa) when they share a single `Endpoint`.
struct SharedEndpoint(Arc<Endpoint>);

impl Clone for SharedEndpoint {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Deref for SharedEndpoint {
    type Target = Endpoint;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for SharedEndpoint {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) == 1 {
            Endpoint::close(&self.0, VarInt::from(0u32), b"endpoint shutdown");
        }
    }
}

/// [`Quinn`] is an implementation of [`StreamLayer`] based on [`quinn`].
pub struct Quinn<R> {
  opts: QuinnOptions,
  _m: PhantomData<R>,
}

impl<R> Quinn<R> {
  /// Creates a new [`Quinn`] stream layer with the given options.
  fn new_in(opts: Options) -> Self {
    Self {
      opts: opts.into(),
      _m: PhantomData,
    }
  }
}

impl<R: Runtime> StreamLayer for Quinn<R> {
  type Runtime = R;
  type Acceptor = QuinnAcceptor;
  type Connector = QuinnConnector<R>;
  type Connection = QuinnConnection;
  type Stream = QuinnStream;
  type Options = Options;

  fn max_stream_data(&self) -> usize {
    self.opts.max_stream_data.min(self.opts.max_connection_data)
  }

  async fn new(opts: Self::Options) -> io::Result<Self> {
    Ok(Self::new_in(opts))
  }

  async fn bind(
    &self,
    addr: SocketAddr,
  ) -> io::Result<(SocketAddr, Self::Acceptor, Self::Connector)> {
    let server_name = self.opts.server_name.clone();

    let client_config = self.opts.client_config.clone();
    let sock = std::net::UdpSocket::bind(addr)?;
    let auto_port = addr.port() == 0;

    let shared_ep = SharedEndpoint(Arc::new(Endpoint::new(
      self.opts.endpoint_config.clone(),
      Some(self.opts.server_config.clone()),
      sock,
      Arc::new(R::quinn()),
    )?));

    let local_addr = shared_ep.local_addr()?;
    if auto_port {
      tracing::info!(
        "memberlist_quic.endpoint: binding to dynamic addr {}",
        local_addr
      );
    }

    let acceptor = Self::Acceptor {
      endpoint: shared_ep.clone(),
      local_addr,
    };

    let connector = Self::Connector {
      server_name,
      endpoint: shared_ep,
      local_addr,
      client_config,
      connect_timeout: self.opts.connect_timeout,
      _marker: PhantomData,
    };
    Ok((local_addr, acceptor, connector))
  }
}

/// [`QuinnAcceptor`] is an implementation of [`QuicAcceptor`] based on [`quinn`].
pub struct QuinnAcceptor {
  endpoint: SharedEndpoint,
  local_addr: SocketAddr,
}

impl Clone for QuinnAcceptor {
  fn clone(&self) -> Self {
    Self {
      endpoint: self.endpoint.clone(),
      local_addr: self.local_addr,
    }
  }
}

impl QuicAcceptor for QuinnAcceptor {
  type Connection = QuinnConnection;

  async fn accept(&mut self) -> io::Result<(Self::Connection, SocketAddr)> {
    let conn = self
      .endpoint
      .accept()
      .await
      .ok_or(io::Error::other("endpoint closed"))?
      .await?;
    let remote_addr = conn.remote_address();

    Ok((
      QuinnConnection::new(conn, self.local_addr, remote_addr),
      remote_addr,
    ))
  }

  async fn close(&mut self) -> io::Result<()> {
    self.endpoint.close(VarInt::from(0u32), b"close acceptor");
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// [`QuinnConnector`] is an implementation of [`QuicConnector`] based on [`quinn`].
pub struct QuinnConnector<R> {
  server_name: SmolStr,
  endpoint: SharedEndpoint,
  client_config: ClientConfig,
  connect_timeout: Duration,
  local_addr: SocketAddr,
  _marker: PhantomData<R>,
}

impl<R> QuicConnector for QuinnConnector<R>
where
  R: Runtime,
{
  type Connection = QuinnConnection;

  async fn connect(&self, addr: SocketAddr) -> io::Result<Self::Connection> {
    let connecting = self
      .endpoint
      .connect_with(self.client_config.clone(), addr, &self.server_name)
      .map_err(|_| io::Error::from(io::ErrorKind::NotConnected))?;
    let conn = R::timeout(self.connect_timeout, connecting)
      .await
      .map_err(io::Error::from)??;
    Ok(QuinnConnection::new(conn, self.local_addr, addr))
  }

  async fn close(&self) -> io::Result<()> {
    self.endpoint.close(VarInt::from(0u32), b"close connector");
    Ok(())
  }

  async fn wait_idle(&self) -> io::Result<()> {
    self.endpoint.wait_idle().await;
    Ok(())
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// A [`ProtoReader`](memberlist_core::proto::ProtoReader) implementation for Quinn stream layer
pub struct QuinnProtoReader {
  stream: RecvStream,
  peek_buf: MediumVec<u8>,
}

impl From<RecvStream> for QuinnProtoReader {
  fn from(stream: RecvStream) -> Self {
    Self {
      stream,
      peek_buf: MediumVec::new(),
    }
  }
}

impl memberlist_core::proto::ProtoReader for QuinnProtoReader {
  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let dst_len = buf.len();
    let peek_len = self.peek_buf.len();

    match dst_len.cmp(&peek_len) {
      cmp::Ordering::Less => {
        buf.copy_from_slice(&self.peek_buf[..dst_len]);
        Ok(dst_len)
      }
      cmp::Ordering::Equal => {
        buf.copy_from_slice(&self.peek_buf);
        Ok(peek_len)
      }
      cmp::Ordering::Greater => {
        let want = dst_len - peek_len;
        self.peek_buf.resize(dst_len, 0);
        match self
          .stream
          .read(&mut self.peek_buf[peek_len..peek_len + want])
          .await
        {
          Ok(Some(n)) => {
            let has = peek_len + n;
            if n < want {
              self.peek_buf.truncate(has);
            }
            buf[..has].copy_from_slice(&self.peek_buf);
            Ok(peek_len + n)
          }
          Ok(None) | Err(_) => {
            self.peek_buf.truncate(peek_len);
            buf[..peek_len].copy_from_slice(&self.peek_buf);
            Ok(peek_len)
          }
        }
      }
    }
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    let dst_len = buf.len();
    let peek_len = self.peek_buf.len();

    match dst_len.cmp(&peek_len) {
      cmp::Ordering::Less => {
        buf.copy_from_slice(&self.peek_buf[..dst_len]);
        Ok(())
      }
      cmp::Ordering::Equal => {
        buf.copy_from_slice(&self.peek_buf);
        Ok(())
      }
      cmp::Ordering::Greater => {
        self.peek_buf.resize(dst_len, 0);
        let mut total = peek_len;
        while total < dst_len {
          let n = self
            .stream
            .read(&mut self.peek_buf[total..])
            .await?
            .unwrap_or(0);
          if n == 0 {
            return Err(std::io::Error::new(
              std::io::ErrorKind::UnexpectedEof,
              "unexpected eof",
            ));
          }
          total += n;
        }
        buf.copy_from_slice(&self.peek_buf);
        Ok(())
      }
    }
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    let dst_len = buf.len();
    let peek_len = self.peek_buf.len();

    if dst_len <= peek_len {
      buf.copy_from_slice(&self.peek_buf[..dst_len]);
      self.peek_buf.drain(..dst_len);
      Ok(dst_len)
    } else {
      buf[..peek_len].copy_from_slice(&self.peek_buf);
      self.peek_buf.clear();
      let mut total = peek_len;
      while total < dst_len {
        let n = self.stream.read(&mut buf[total..]).await?.unwrap_or(0);
        if n == 0 {
          break;
        }
        total += n;
      }
      Ok(total)
    }
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    let dst_len = buf.len();
    let peek_len = self.peek_buf.len();

    if dst_len <= peek_len {
      buf.copy_from_slice(&self.peek_buf[..dst_len]);
      self.peek_buf.drain(..dst_len);
      Ok(())
    } else {
      buf[..peek_len].copy_from_slice(&self.peek_buf);
      self.peek_buf.clear();
      let mut total = peek_len;
      while total < dst_len {
        let n = self.stream.read(&mut buf[total..]).await?.unwrap_or(0);
        if n == 0 {
          return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected eof",
          ));
        }
        total += n;
      }
      Ok(())
    }
  }
}

/// [`QuinnStream`] is an implementation of [`QuicStream`] based on [`quinn`].
pub struct QuinnStream {
  send: SendStream,
  recv: QuinnProtoReader,
}

impl QuinnStream {
  #[inline]
  fn new(send: SendStream, recv: RecvStream) -> Self {
    Self {
      send,
      recv: recv.into(),
    }
  }
}

impl memberlist_core::transport::Connection for QuinnStream {
  type Reader = QuinnProtoReader;

  type Writer = SendStream;

  fn split(self) -> (Self::Reader, Self::Writer) {
    (self.recv, self.send)
  }

  async fn close(&mut self) -> std::io::Result<()> {
    self.send.close().await
  }

  async fn write_all(&mut self, payload: &[u8]) -> std::io::Result<()> {
    self.send.write_all(payload).await.map_err(Into::into)
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    self.send.flush().await
  }

  async fn peek(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    memberlist_core::proto::ProtoReader::peek(&mut self.recv, buf).await
  }

  async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    memberlist_core::proto::ProtoReader::read_exact(&mut self.recv, buf).await
  }

  async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
    memberlist_core::proto::ProtoReader::read(&mut self.recv, buf).await
  }

  async fn peek_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
    memberlist_core::proto::ProtoReader::peek_exact(&mut self.recv, buf).await
  }

  fn consume_peek(&mut self) {
    self.recv.peek_buf.clear();
  }
}

impl QuicStream for QuinnStream {
  type SendStream = SendStream;

  async fn read_packet(&mut self) -> std::io::Result<bytes::Bytes> {
    // TODO(al8n): make size limit configurable?
    self
      .recv
      .stream
      .read_to_end(u32::MAX as usize)
      .await
      .map(|data| {
        if !self.recv.peek_buf.is_empty() {
          let mut buf = bytes::BytesMut::with_capacity(self.recv.peek_buf.len() + data.len());
          buf.extend_from_slice(&self.recv.peek_buf);
          buf.extend_from_slice(&data);
          buf.freeze()
        } else {
          data.into()
        }
      })
      .map_err(|e| match e {
        quinn::ReadToEndError::Read(e) => std::io::Error::from(e),
        quinn::ReadToEndError::TooLong => {
          std::io::Error::new(std::io::ErrorKind::InvalidData, "packet too large")
        }
      })
  }
}

/// A connection based on [`quinn`].
pub struct QuinnConnection {
  conn: Connection,
  local_addr: SocketAddr,
  remote_addr: SocketAddr,
}

impl Clone for QuinnConnection {
  fn clone(&self) -> Self {
    Self {
      conn: self.conn.clone(),
      local_addr: self.local_addr,
      remote_addr: self.remote_addr,
    }
  }
}

impl QuinnConnection {
  #[inline]
  fn new(conn: Connection, local_addr: SocketAddr, remote_addr: SocketAddr) -> Self {
    Self {
      conn,
      local_addr,
      remote_addr,
    }
  }
}

impl QuicConnection for QuinnConnection {
  type Stream = QuinnStream;

  async fn accept_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    let (send, recv) = self.conn.accept_bi().await?;
    Ok((QuinnStream::new(send, recv), self.remote_addr))
  }

  async fn open_bi(&self) -> io::Result<(Self::Stream, SocketAddr)> {
    let (send, recv) = self.conn.open_bi().await?;
    Ok((QuinnStream::new(send, recv), self.remote_addr))
  }

  async fn close(&self) -> io::Result<()> {
    self.conn.close(0u32.into(), b"close connection");
    Ok(())
  }

  async fn is_closed(&self) -> bool {
    self.conn.close_reason().is_some()
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}
