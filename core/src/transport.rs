#[cfg(feature = "nightly")]
use std::future::Future;
use std::{
  net::SocketAddr,
  time::{Duration, Instant},
};

use crate::types::{
  DecodeError, DecodeU32Error, EncodeError, InvalidLabel, Label, MessageType, Packet,
};

use bytes::{BufMut, Bytes, BytesMut};

pub mod stream;
use stream::*;

pub mod net;

#[cfg(feature = "test")]
pub(crate) mod tests;

const LABEL_MAX_SIZE: usize = 255;
const DEFAULT_BUFFER_SIZE: usize = 4096;
const MAX_PUSH_STATE_BYTES: usize = 20 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionKind {
  Reliable,
  Unreliable,
}

impl core::fmt::Display for ConnectionKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionKind {
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      ConnectionKind::Reliable => "reliable",
      ConnectionKind::Unreliable => "unreliable",
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionErrorKind {
  Accept,
  Close,
  Dial,
  Flush,
  Read,
  Write,
  Label,
}

impl core::fmt::Display for ConnectionErrorKind {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl ConnectionErrorKind {
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Accept => "accept",
      Self::Read => "read",
      Self::Write => "write",
      Self::Dial => "dial",
      Self::Flush => "flush",
      Self::Close => "close",
      Self::Label => "label",
    }
  }
}

#[viewit::viewit(vis_all = "pub")]
#[derive(Debug)]
pub struct ConnectionError {
  kind: ConnectionKind,
  error_kind: ConnectionErrorKind,
  error: std::io::Error,
}

impl core::fmt::Display for ConnectionError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "{} connection {} error {}",
      self.kind.as_str(),
      self.error_kind.as_str(),
      self.error
    )
  }
}

impl std::error::Error for ConnectionError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    Some(&self.error)
  }
}

impl ConnectionError {
  fn failed_remote(&self) -> bool {
    #[allow(clippy::match_like_matches_macro)]
    match self.kind {
      ConnectionKind::Reliable => match self.error_kind {
        ConnectionErrorKind::Read | ConnectionErrorKind::Write | ConnectionErrorKind::Dial => true,
        _ => false,
      },
      ConnectionKind::Unreliable => match self.error_kind {
        ConnectionErrorKind::Write => true,
        _ => false,
      },
    }
  }
}

#[derive(thiserror::Error)]
pub enum TransportError<T: Transport> {
  #[error("connection error: {0}")]
  Connection(#[from] ConnectionError),
  #[error("encode error: {0}")]
  Encode(#[from] EncodeError),
  #[error("decode error: {0}")]
  Decode(#[from] DecodeError),
  #[error("security error {0}")]
  Security(#[from] crate::security::SecurityError),
  #[error("remote node state(size {0}) is larger than limit (20 MB)")]
  RemoteStateTooLarge(usize),
  #[error("other: {0}")]
  Other(T::Error),
}

impl<T: Transport> core::fmt::Debug for TransportError<T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{self}")
  }
}

impl<T: Transport> TransportError<T> {
  pub(crate) fn failed_remote(&self) -> bool {
    if let Self::Connection(e) = self {
      e.failed_remote()
    } else {
      false
    }
  }
}

pub use r#async::*;

mod r#async {
  use std::{
    io,
    net::IpAddr,
    sync::Arc,
    task::{Context, Poll},
  };

  use crate::{
    checksum::Checksumer,
    types::{
      Compress, EncodeHeader, EncodeMeta, ENCODE_HEADER_SIZE, ENCODE_META_SIZE, MAX_MESSAGE_SIZE,
    },
  };

  use super::*;
  use bytes::Buf;
  use futures_util::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    Future,
  };
  use nodecraft::{resolver::AddressResolver, Id};

  pub struct ReliableConnection<T: Transport>(BufReader<T::Connection>, SocketAddr);

  impl<T: Transport> AsRef<ReliableConnection<T>> for ReliableConnection<T> {
    #[inline]
    fn as_ref(&self) -> &ReliableConnection<T> {
      self
    }
  }

  #[allow(dead_code)]
  impl<T> ReliableConnection<T>
  where
    T: Transport,
  {
    #[inline]
    pub fn new(conn: T::Connection, addr: SocketAddr) -> Self {
      Self(BufReader::with_capacity(DEFAULT_BUFFER_SIZE, conn), addr)
    }

    #[inline]
    pub(crate) fn reader(&mut self) -> &mut BufReader<T::Connection> {
      &mut self.0
    }

    #[inline]
    pub(crate) async fn read_message_header(&mut self) -> Result<EncodeHeader, TransportError<T>> {
      let mut meta = [0; ENCODE_HEADER_SIZE];
      self.read_exact(&mut meta).await?;
      let mt = meta[0].try_into().map_err(DecodeError::from)?;
      let marker = meta[1];
      let msgs = meta[2];
      let r1 = meta[3];
      let len = u32::from_be_bytes(
        (meta[ENCODE_META_SIZE..ENCODE_META_SIZE + MAX_MESSAGE_SIZE].try_into()).unwrap(),
      );
      Ok(EncodeHeader {
        meta: EncodeMeta {
          ty: mt,
          marker,
          msgs,
          r1,
        },
        len,
      })
    }

    #[inline]
    pub(crate) async fn read_message(
      &mut self,
    ) -> Result<(EncodeHeader, Bytes), TransportError<T>> {
      let header = self.read_message_header().await?;
      let mut buf = vec![0; header.len as usize];
      self.read_exact(&mut buf).await?;
      Ok((header, buf.into()))
    }

    #[inline]
    pub(crate) async fn read_encrypt_message(
      &mut self,
      header: EncodeHeader,
      #[cfg(feature = "metrics")] metric_labels: &[metrics::Label],
    ) -> Result<BytesMut, TransportError<T>> {
      // Ensure we aren't asked to download too much. This is to guard against
      // an attack vector where a huge amount of state is sent
      let more_bytes = header.len as usize;
      #[cfg(feature = "metrics")]
      {
        crate::network::sealed_metrics::add_sample_to_remote_size_histogram(
          more_bytes as f64,
          metric_labels.iter(),
        );
      }

      if more_bytes > MAX_PUSH_STATE_BYTES {
        return Err(TransportError::RemoteStateTooLarge(more_bytes));
      }

      // Start reporting the size before you cross the limit
      if more_bytes > (0.6 * (MAX_PUSH_STATE_BYTES as f64)).floor() as usize {
        tracing::warn!(
          target = "showbiz",
          "remote state size is {} limit is large: {}",
          more_bytes,
          MAX_PUSH_STATE_BYTES
        );
      }

      let mut buf = BytesMut::with_capacity(ENCODE_HEADER_SIZE + header.len as usize);
      buf.put_slice(&header.to_array());
      buf.resize(ENCODE_HEADER_SIZE + header.len as usize, 0);
      self.read_exact(&mut buf[ENCODE_HEADER_SIZE..]).await?;
      Ok(buf)
    }

    #[inline]
    pub(crate) async fn read_compressed_message(
      &mut self,
      header: &EncodeHeader,
    ) -> Result<Compress, TransportError<T>> {
      let mut buf = vec![0; header.len as usize];
      self.read_exact(&mut buf).await?;
      let mut buf: Bytes = buf.into();
      let algo = buf.get_u8().try_into().map_err(DecodeError::from)?;
      Ok(Compress { algo, buf })
    }

    #[inline]
    pub(crate) async fn read_u32_varint(&mut self) -> Result<usize, TransportError<T>> {
      let mut n = 0;
      let mut shift = 0;
      for _ in 0..5 {
        let mut byte = [0; 1];
        self.read_exact(&mut byte).await?;
        let b = byte[0];

        if b < 0x80 {
          return Ok((n | ((b as u32) << shift)) as usize);
        }

        n |= ((b & 0x7f) as u32) << shift;
        shift += 7;
      }

      Err(TransportError::Decode(DecodeError::Length(DecodeU32Error)))
    }

    #[inline]
    pub fn with_capacity(capacity: usize, conn: T::Connection, addr: SocketAddr) -> Self {
      Self(BufReader::with_capacity(capacity, conn), addr)
    }

    #[inline]
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError<T>> {
      self.0.read(buf).await.map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Read,
          error: e,
        })
      })
    }

    #[inline]
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError<T>> {
      self.0.read_exact(buf).await.map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Read,
          error: e,
        })
      })
    }

    #[inline]
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError<T>> {
      self.0.write(buf).await.map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Write,
          error: e,
        })
      })
    }

    #[inline]
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError<T>> {
      self.0.write_all(buf).await.map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Write,
          error: e,
        })
      })
    }

    #[inline]
    pub async fn flush(&mut self) -> Result<(), TransportError<T>> {
      self.0.flush().await.map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Flush,
          error: e,
        })
      })
    }

    #[inline]
    pub async fn close(&mut self) -> Result<(), TransportError<T>> {
      self.0.close().await.map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Reliable,
          error_kind: ConnectionErrorKind::Write,
          error: e,
        })
      })
    }

    #[inline]
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
      self.0.get_mut().set_timeout(timeout)
    }

    #[inline]
    pub fn timeout(&self) -> (Option<Duration>, Option<Duration>) {
      self.0.get_ref().timeout()
    }

    #[inline]
    pub fn remote_node(&self) -> SocketAddr {
      self.1
    }

    /// General approach is to prefix with the same structure:
    ///
    /// magic type byte (244): `u8`
    /// length of label name:  `u8` (because labels can't be longer than 255 bytes)
    /// label name:            `Vec<u8>`
    ///
    /// Write a label header.
    pub async fn add_label_header(&mut self, label: &[u8]) -> Result<(), TransportError<T>> {
      if label.is_empty() {
        return Ok(());
      }

      if label.len() > LABEL_MAX_SIZE {
        return Err(TransportError::Encode(EncodeError::InvalidLabel(
          InvalidLabel::InvalidSize(label.len()),
        )));
      }

      let mut bytes = BytesMut::with_capacity(label.len() + 2);
      bytes.put_u8(MessageType::HasLabel as u8);
      bytes.put_u8(label.len() as u8);
      bytes.put_slice(label);
      self.write_all(&bytes).await
    }

    /// Removes any label header from the beginning of
    /// the stream if present and returns it.
    pub async fn remove_label_header(&mut self) -> Result<Label, TransportError<T>> {
      let buf = match self.0.fill_buf().await {
        Ok(buf) => {
          if buf.is_empty() {
            return Ok(Label::empty());
          }
          buf
        }
        Err(e) => {
          return if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(Label::empty())
          } else {
            Err(TransportError::Connection(ConnectionError {
              kind: ConnectionKind::Reliable,
              error_kind: ConnectionErrorKind::Read,
              error: e,
            }))
          }
        }
      };

      // First check for the type byte.
      match MessageType::try_from(buf[0]) {
        Ok(MessageType::HasLabel) => {}
        Ok(_) => return Ok(Label::empty()),
        Err(e) => return Err(TransportError::Decode(DecodeError::InvalidMessageType(e))),
      }

      if buf.len() < 2 {
        return Err(TransportError::Decode(DecodeError::Truncated("label")));
      }
      let label_size = buf[1] as usize;
      if label_size < 1 {
        return Err(TransportError::Decode(DecodeError::InvalidLabel(
          InvalidLabel::InvalidSize(0),
        )));
      }

      if buf.len() < 2 + label_size {
        return Err(TransportError::Decode(DecodeError::Truncated("label")));
      }

      let label = Bytes::copy_from_slice(&buf[2..2 + label_size]);
      self.0.consume_unpin(2 + label_size);

      Label::from_bytes(label).map_err(|e| TransportError::Decode(DecodeError::InvalidLabel(e)))
    }
  }

  pub struct UnreliableConnection<T: Transport>(T::UnreliableConnection);

  impl<T: Transport> AsRef<UnreliableConnection<T>> for UnreliableConnection<T> {
    #[inline]
    fn as_ref(&self) -> &UnreliableConnection<T> {
      self
    }
  }

  impl<T> UnreliableConnection<T>
  where
    T: Transport,
  {
    #[inline]
    pub fn new(conn: T::UnreliableConnection) -> Self {
      Self(conn)
    }

    #[inline]
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
      self.0.set_timeout(timeout)
    }

    #[inline]
    pub fn timeout(&self) -> (Option<Duration>, Option<Duration>) {
      self.0.timeout()
    }

    #[inline]
    pub async fn send_to(&self, addr: SocketAddr, buf: &[u8]) -> Result<usize, TransportError<T>> {
      PacketConnection::send_to(&self.0, addr, buf)
        .await
        .map_err(|e| {
          TransportError::Connection(ConnectionError {
            kind: ConnectionKind::Unreliable,
            error_kind: ConnectionErrorKind::Write,
            error: e,
          })
        })
    }

    #[inline]
    pub async fn recv_from(
      &self,
      buf: &mut [u8],
    ) -> Result<(usize, SocketAddr), TransportError<T>> {
      PacketConnection::recv_from(&self.0, buf)
        .await
        .map_err(|e| {
          TransportError::Connection(ConnectionError {
            kind: ConnectionKind::Unreliable,
            error_kind: ConnectionErrorKind::Read,
            error: e,
          })
        })
    }

    pub fn poll_recv_from(
      &self,
      cx: &mut Context<'_>,
      buf: &mut [u8],
    ) -> Poll<Result<(usize, SocketAddr), TransportError<T>>> {
      PacketConnection::poll_recv_from(&self.0, cx, buf).map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Unreliable,
          error_kind: ConnectionErrorKind::Read,
          error: e,
        })
      })
    }

    pub fn poll_send_to(
      &self,
      cx: &mut Context<'_>,
      buf: &[u8],
      target: SocketAddr,
    ) -> Poll<Result<usize, TransportError<T>>> {
      PacketConnection::poll_send_to(&self.0, cx, buf, target).map_err(|e| {
        TransportError::Connection(ConnectionError {
          kind: ConnectionKind::Unreliable,
          error_kind: ConnectionErrorKind::Write,
          error: e,
        })
      })
    }
  }

  /// Compressor is used to compress and decompress data from a transport connection.
  pub trait Compressor {
    /// The error type returned by the compressor.
    type Error: std::error::Error;

    /// Compress data from a slice, returning compressed data.
    fn compress(&self, buf: &[u8]) -> Vec<u8>;

    /// Compress data from a slice, writing the compressed data to the given writer.
    async fn compress_to_writer<W: futures_util::io::AsyncWrite>(
      &self,
      buf: &[u8],
      writer: W,
    ) -> Result<(), Self::Error>;

    /// Decompress data from a slice, returning uncompressed data.
    fn decompress(src: &[u8]) -> Result<Vec<u8>, Self::Error>;

    /// Decompress data from a reader, returning the bytes readed and the uncompressed data.
    async fn decompress_from_reader<R: futures_util::io::AsyncRead>(
      reader: R,
    ) -> Result<(usize, Vec<u8>), Self::Error>;
  }

  pub trait ConnectionTimeout: Unpin + Send + Sync + 'static {
    fn set_write_timeout(&self, timeout: Option<Duration>);

    fn write_timeout(&self) -> Option<Duration>;

    fn set_read_timeout(&self, timeout: Option<Duration>);

    fn read_timeout(&self) -> Option<Duration>;

    fn set_timeout(&self, timeout: Option<Duration>) {
      Self::set_read_timeout(self, timeout);
      Self::set_write_timeout(self, timeout);
    }

    fn timeout(&self) -> (Option<Duration>, Option<Duration>) {
      (Self::read_timeout(self), Self::write_timeout(self))
    }
  }

  pub trait PacketConnection: ConnectionTimeout + Send + Sync + 'static {
    async fn send_to(&self, addr: SocketAddr, buf: &[u8]) -> std::io::Result<usize>;

    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)>;

    #[cfg(feature = "nightly")]
    fn send_to<'a>(
      &'a self,
      addr: SocketAddr,
      buf: &'a [u8],
    ) -> impl futures_util::Future<Output = Result<usize, std::io::Error>> + Send + 'a;

    #[cfg(feature = "nightly")]
    fn recv_from<'a>(
      &'a self,
      buf: &'a mut [u8],
    ) -> impl futures_util::Future<Output = Result<(usize, SocketAddr), std::io::Error>> + Send + 'a;

    /// Attempts to receive a single datagram on the socket.
    ///
    /// Note that on multiple calls to a `poll_*` method in the recv direction, only the
    /// `Waker` from the `Context` passed to the most recent call will be scheduled to
    /// receive a wakeup.
    ///
    /// # Return value
    ///
    /// The function returns:
    ///
    /// * `Poll::Pending` if the socket is not ready to read
    /// * `Poll::Ready(Ok(addr))` reads data from `addr` into `ReadBuf` if the socket is ready
    /// * `Poll::Ready(Err(e))` if an error is encountered.
    ///
    /// # Errors
    ///
    /// This function may encounter any standard I/O error except `WouldBlock`.
    ///
    /// # Notes
    /// Note that the socket address **cannot** be implicitly trusted, because it is relatively
    /// trivial to send a UDP datagram with a spoofed origin in a [packet injection attack].
    /// Because UDP is stateless and does not validate the origin of a packet,
    /// the attacker does not need to be able to intercept traffic in order to interfere.
    /// It is important to be aware of this when designing your application-level protocol.
    ///
    /// [packet injection attack]: https://en.wikipedia.org/wiki/Packet_injection
    fn poll_recv_from(
      &self,
      cx: &mut Context<'_>,
      buf: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>>;

    /// Attempts to send data on the socket to a given address.
    ///
    /// Note that on multiple calls to a `poll_*` method in the send direction, only the
    /// `Waker` from the `Context` passed to the most recent call will be scheduled to
    /// receive a wakeup.
    ///
    /// # Return value
    ///
    /// The function returns:
    ///
    /// * `Poll::Pending` if the socket is not ready to write
    /// * `Poll::Ready(Ok(n))` `n` is the number of bytes sent.
    /// * `Poll::Ready(Err(e))` if an error is encountered.
    ///
    /// # Errors
    ///
    /// This function may encounter any standard I/O error except `WouldBlock`.
    fn poll_send_to(
      &self,
      cx: &mut Context<'_>,
      buf: &[u8],
      target: SocketAddr,
    ) -> Poll<io::Result<usize>>;
  }

  pub trait TransportOptions:
    Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
  {
    fn from_addr(addr: IpAddr, port: Option<u16>) -> Self;

    fn from_addrs(addrs: impl Iterator<Item = IpAddr>, port: Option<u16>) -> Self;
  }

  impl<T: TransportOptions> TransportOptions for Arc<T> {
    fn from_addr(addr: IpAddr, port: Option<u16>) -> Self {
      Arc::new(T::from_addr(addr, port))
    }

    fn from_addrs(addrs: impl Iterator<Item = IpAddr>, port: Option<u16>) -> Self {
      Arc::new(T::from_addrs(addrs, port))
    }
  }

  impl<T: TransportOptions> TransportOptions for Box<T> {
    fn from_addr(addr: IpAddr, port: Option<u16>) -> Self {
      Box::new(T::from_addr(addr, port))
    }

    fn from_addrs(addrs: impl Iterator<Item = IpAddr>, port: Option<u16>) -> Self {
      Box::new(T::from_addrs(addrs, port))
    }
  }

  /// Transport is used to abstract over communicating with other peers. The packet
  /// interface is assumed to be best-effort and the stream interface is assumed to
  /// be reliable.
  pub trait Transport: Sized + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Id: Id;
    type Resolver: AddressResolver;
    type Checksumer: Checksumer + Send + Sync + 'static;
    type Connection: ConnectionTimeout + futures_util::io::AsyncRead + futures_util::io::AsyncWrite;
    type UnreliableConnection: PacketConnection;
    type Options: TransportOptions;
    type Runtime: agnostic::Runtime;

    /// Creates a new transport instance with the given options
    fn new(
      label: Option<Label>,
      resolver: Self::Resolver,
      opts: Self::Options,
      runtime: Self::Runtime,
    ) -> impl Future<Output = Result<Self, TransportError<Self>>> + Send
    where
      Self: Sized;

    /// Creates a new transport instance with the given options and metrics labels
    #[cfg(feature = "metrics")]
    fn with_metric_labels(
      label: Option<Label>,
      resolver: Self::Resolver,
      opts: Self::Options,
      metric_labels: std::sync::Arc<Vec<metrics::Label>>,
    ) -> impl Future<Output = Result<Self, TransportError<Self>>> + Send
    where
      Self: Sized;

    // /// Given the user's configured values (which
    // /// might be empty) and returns the desired IP and port to advertise to
    // /// the rest of the cluster.
    // fn final_advertise_addr(
    //   &self,
    //   addr: Option<IpAddr>,
    //   port: u16,
    // ) -> Result<SocketAddr, TransportError<Self>>;

    // /// Returns the bind port that was automatically given by the
    // /// kernel, if a bind port of 0 was given.
    // fn auto_bind_port(&self) -> u16;

    /// A packet-oriented interface that fires off the given
    /// payload to the given address in a connectionless fashion. This should
    /// return a time stamp that's as close as possible to when the packet
    /// was transmitted to help make accurate RTT measurements during probes.
    ///
    /// This is similar to net.PacketConn, though we didn't want to expose
    /// that full set of required methods to keep assumptions about the
    /// underlying plumbing to a minimum. We also treat the address here as a
    /// string, similar to Dial, so it's network neutral, so this usually is
    /// in the form of "host:port".
    fn write_to(
      &self,
      b: &[u8],
      addr: SocketAddr,
    ) -> impl Future<Output = Result<Instant, TransportError<Self>>> + Send;

    /// Used to create a connection that allows us to perform
    /// two-way communication with a peer. This is generally more expensive
    /// than packet connections so is used for more infrequent operations
    /// such as anti-entropy or fallback probes if the packet-oriented probe
    /// failed.
    fn dial_timeout(
      &self,
      addr: SocketAddr,
      timeout: Duration,
    ) -> impl Future<Output = Result<ReliableConnection<Self>, TransportError<Self>>> + Send;

    fn packet(&self) -> PacketSubscriber;

    /// Returns a receiver that can be read to handle incoming stream
    /// connections from other peers. How this is set up for listening is
    /// left as an exercise for the concrete transport implementations.
    fn stream(&self) -> ConnectionSubscriber<Self>;

    /// Shutdown the transport
    fn shutdown(&self) -> impl Future<Output = Result<(), TransportError<Self>>> + Send;

    /// Blocking shutdown the transport
    fn block_shutdown(&self) -> Result<(), TransportError<Self>>;
  }
}
