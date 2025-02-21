//! [`memberlist`](https://crates.io/crates/memberlist)'s [`Transport`] layer based on QUIC.
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![allow(clippy::type_complexity)]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  net::{IpAddr, SocketAddr},
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use agnostic_lite::{time::Instant, AsyncSpawner, RuntimeLite};
use atomic_refcell::AtomicRefCell;
use crossbeam_skiplist::SkipMap;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use memberlist_core::proto::{Data, Payload, SmallVec};
pub use memberlist_core::{
  proto::{CIDRsPolicy, Label, LabelError, ProtoReader},
  transport::*,
};

mod processor;
use processor::*;

/// Exports unit tests.
#[cfg(any(test, feature = "test"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests;

mod error;
pub use error::*;
mod options;
pub use options::*;
/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`QuicTransport`].
pub mod stream_layer;
use stream_layer::*;

const MAX_MESSAGE_SIZE: usize = u32::MAX as usize;

const PACKET_TAG: u8 = 254;
const STREAM_TAG: u8 = 255;

#[derive(Copy, Clone)]
#[repr(u8)]
enum StreamType {
  Stream = STREAM_TAG,
  Packet = PACKET_TAG,
}

impl TryFrom<u8> for StreamType {
  type Error = u8;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      STREAM_TAG => Self::Stream,
      PACKET_TAG => Self::Packet,
      _ => return Err(value),
    })
  }
}

#[cfg(feature = "tokio")]
/// [`QuicTransport`] based on [`tokio`](https://crates.io/crates/tokio).
pub type TokioQuicTransport<I, A, S> = QuicTransport<I, A, S, agnostic_lite::tokio::TokioRuntime>;

#[cfg(feature = "async-std")]
/// [`QuicTransport`] based on [`async-std`](https://crates.io/crates/async-std).
pub type AsyncStdQuicTransport<I, A, S> =
  QuicTransport<I, A, S, agnostic_lite::async_std::AsyncStdRuntime>;

#[cfg(feature = "smol")]
/// [`QuicTransport`] based on [`smol`](https://crates.io/crates/smol).
pub type SmolQuicTransport<I, A, S> = QuicTransport<I, A, S, agnostic_lite::smol::SmolRuntime>;

/// A [`Transport`] implementation based on QUIC
pub struct QuicTransport<I, A, S, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  opts: Options<I, A>,
  advertise_addr: A::ResolvedAddress,
  local_addr: A::Address,
  packet_rx: PacketSubscriber<A::ResolvedAddress, R::Instant>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  #[allow(dead_code)]
  stream_layer: S,
  connection_pool: Arc<SkipMap<SocketAddr, (R::Instant, S::Connection)>>,
  v4_round_robin: AtomicUsize,
  v4_connectors: SmallVec<S::Connector>,
  v6_round_robin: AtomicUsize,
  v6_connectors: SmallVec<S::Connector>,
  handles: AtomicRefCell<FuturesUnordered<<R::Spawner as AsyncSpawner>::JoinHandle<()>>>,
  resolver: A,
  shutdown_tx: async_channel::Sender<()>,
  max_packet_size: usize,
}

impl<I, A, S, R> QuicTransport<I, A, S, R>
where
  I: Id + Data + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  A::ResolvedAddress: Data,
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  async fn new_in(
    resolver: A,
    stream_layer: S,
    opts: Options<I, A>,
  ) -> Result<Self, QuicTransportError<A>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addresses.is_empty() {
      return Err(QuicTransportError::EmptyBindAddresses);
    }

    let (stream_tx, stream_rx) = promised_stream::<Self>();
    let (packet_tx, packet_rx) = packet_stream::<Self>();
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut v4_connectors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut v6_connectors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut v4_acceptors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut v6_acceptors = SmallVec::with_capacity(opts.bind_addresses.len());
    let mut resolved_bind_address = SmallVec::new();

    for addr in opts.bind_addresses.iter() {
      let addr = resolver
        .resolve(addr)
        .await
        .map_err(|e| QuicTransportError::Resolve {
          addr: addr.cheap_clone(),
          err: e,
        })?;

      let bind_port = addr.port();

      let (local_addr, acceptor, connector) = if bind_port == 0 {
        let mut retries = 0;
        loop {
          match stream_layer.bind(addr).await {
            Ok(res) => break res,
            Err(e) => {
              if retries < 9 {
                retries += 1;
                continue;
              }
              return Err(QuicTransportError::Listen(addr, e));
            }
          }
        }
      } else {
        match stream_layer.bind(addr).await {
          Ok(res) => res,
          Err(e) => return Err(QuicTransportError::Listen(addr, e)),
        }
      };

      if local_addr.is_ipv4() {
        v4_acceptors.push((local_addr, acceptor));
        v4_connectors.push(connector);
      } else {
        v6_acceptors.push((local_addr, acceptor));
        v6_connectors.push(connector);
      }
      // If the config port given was zero, use the first TCP listener
      // to pick an available port and then apply that to everything
      // else.
      let addr = if bind_port == 0 { local_addr } else { addr };
      resolved_bind_address.push(addr);
    }

    let expose_addr_index = Self::find_advertise_addr_index(&resolved_bind_address);
    let advertise_addr = resolved_bind_address[expose_addr_index];
    let self_addr = opts.bind_addresses[expose_addr_index].cheap_clone();
    let handles = FuturesUnordered::new();

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for (local_addr, acceptor) in v4_acceptors.into_iter().chain(v6_acceptors.into_iter()) {
      let processor = Processor::<A, Self, S> {
        acceptor,
        packet_tx: packet_tx.clone(),
        stream_tx: stream_tx.clone(),
        local_addr,
        timeout: opts.timeout,
        shutdown_rx: shutdown_rx.clone(),
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
      };

      handles.push(R::spawn(processor.run()));
    }

    // find final advertise address
    let final_advertise_addr = if advertise_addr.ip().is_unspecified() {
      let ip = getifs::private_addrs()
        .map_err(|_| QuicTransportError::NoPrivateIP)
        .and_then(|ips| {
          if let Some(ip) = ips.into_iter().next().map(|ip| ip.addr()) {
            Ok(ip)
          } else {
            Err(QuicTransportError::NoPrivateIP)
          }
        })?;
      SocketAddr::new(ip, advertise_addr.port())
    } else {
      advertise_addr
    };

    let connection_pool = Arc::new(SkipMap::new());
    let interval = <A::Runtime as RuntimeLite>::interval(opts.connection_pool_cleanup_period);
    let pool = connection_pool.clone();
    let shutdown_rx = shutdown_rx.clone();
    handles.push(R::spawn(Self::connection_pool_cleaner(
      pool,
      interval,
      shutdown_rx,
      opts.connection_ttl.unwrap_or(Duration::ZERO),
    )));

    Ok(Self {
      advertise_addr: final_advertise_addr,
      connection_pool,
      local_addr: self_addr,
      max_packet_size: MAX_MESSAGE_SIZE.min(stream_layer.max_stream_data()),
      opts,
      packet_rx,
      stream_rx,
      handles: AtomicRefCell::new(handles),
      v4_connectors,
      v6_connectors,
      v4_round_robin: AtomicUsize::new(0),
      v6_round_robin: AtomicUsize::new(0),
      stream_layer,
      resolver,
      shutdown_tx,
    })
  }

  fn find_advertise_addr_index(addrs: &[SocketAddr]) -> usize {
    for (i, addr) in addrs.iter().enumerate() {
      if !addr.ip().is_unspecified() {
        return i;
      }
    }

    0
  }

  async fn connection_pool_cleaner(
    pool: Arc<SkipMap<SocketAddr, (R::Instant, S::Connection)>>,
    mut interval: impl agnostic_lite::time::AsyncInterval,
    shutdown_rx: async_channel::Receiver<()>,
    max_conn_idle: Duration,
  ) {
    loop {
      futures::select! {
        _ = interval.next().fuse() => {
          for ent in pool.iter() {
            let (deadline, conn) = ent.value();
            if max_conn_idle == Duration::ZERO {
              if conn.is_closed().await {
                let _ = conn.close().await;
                ent.remove();
              }
              continue;
            }

            if deadline.elapsed() >= max_conn_idle || conn.is_closed().await {
              let _ = conn.close().await;
              ent.remove();
            }
          }
        }
        _ = shutdown_rx.recv().fuse() => {
          for ent in pool.iter() {
            let _ = ent.value().1.close().await;
          }
          return;
        }
      }
    }
  }
}

impl<I, A, S, R> QuicTransport<I, A, S, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  fn next_connector(&self, addr: &A::ResolvedAddress) -> &S::Connector {
    if addr.is_ipv4() {
      // if there's no v4 sockets, we assume remote addr can accept both v4 and v6
      // give a try on v6
      if self.v4_connectors.is_empty() {
        let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.v6_connectors.len();
        &self.v6_connectors[idx]
      } else {
        let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.v4_connectors.len();
        &self.v4_connectors[idx]
      }
    } else if self.v6_connectors.is_empty() {
      let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.v4_connectors.len();
      &self.v4_connectors[idx]
    } else {
      let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.v6_connectors.len();
      &self.v6_connectors[idx]
    }
  }

  async fn fetch_stream(
    &self,
    addr: SocketAddr,
    timeout: Option<R::Instant>,
  ) -> Result<S::Stream, QuicTransportError<A>> {
    if let Some(ent) = self.connection_pool.get(&addr) {
      let (_, connection) = ent.value();
      if !connection.is_closed().await {
        if let Some(timeout) = timeout {
          return R::timeout_at(timeout, connection.open_bi())
            .await
            .map_err(|e| QuicTransportError::Io(e.into()))?
            .map(|(stream, _)| stream)
            .map_err(Into::into);
        } else {
          return connection
            .open_bi()
            .await
            .map(|(s, _)| s)
            .map_err(Into::into);
        }
      }
    }

    let connector = self.next_connector(&addr);
    let connection = connector.connect(addr).await?;
    connection
      .open_bi()
      .await
      .map(|(s, _)| {
        self
          .connection_pool
          .insert(addr, (Instant::now(), connection));
        s
      })
      .map_err(Into::into)
  }

  async fn fetch_send_stream(
    &self,
    addr: SocketAddr,
    timeout: Option<R::Instant>,
  ) -> Result<<S::Stream as memberlist_core::transport::Connection>::Writer, QuicTransportError<A>>
  {
    if let Some(ent) = self.connection_pool.get(&addr) {
      let (_, connection) = ent.value();
      if !connection.is_closed().await {
        if let Some(timeout) = timeout {
          return R::timeout_at(timeout, connection.open_uni())
            .await
            .map_err(|e| QuicTransportError::Io(e.into()))?
            .map(|(stream, _)| stream)
            .map_err(Into::into);
        } else {
          return connection
            .open_uni()
            .await
            .map(|(s, _)| s)
            .map_err(Into::into);
        }
      }
    }

    let connector = self.next_connector(&addr);
    let connection = connector.connect(addr).await?;
    connection
      .open_uni()
      .await
      .map(|(s, _)| {
        self
          .connection_pool
          .insert(addr, (Instant::now(), connection));
        s
      })
      .map_err(Into::into)
  }
}

impl<I, A, S, R> Transport for QuicTransport<I, A, S, R>
where
  I: Id + Data + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  A::ResolvedAddress: Data,
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  type Error = QuicTransportError<A>;

  type Id = I;

  type Address = A::Address;
  type ResolvedAddress = SocketAddr;
  type Resolver = A;

  type Connection = S::Stream;

  type Runtime = A::Runtime;

  type Options = QuicTransportOptions<I, A, S>;

  async fn new(transport_opts: Self::Options) -> Result<Self, Self::Error> {
    let (resolver_options, stream_layer_options, opts) = transport_opts.into();
    let resolver = <A as AddressResolver>::new(resolver_options)
      .await
      .map_err(Self::Error::Resolver)?;

    let stream_layer = S::new(stream_layer_options).await?;
    Self::new_in(resolver, stream_layer, opts).await
  }

  async fn resolve(
    &self,
    addr: &<Self::Resolver as AddressResolver>::Address,
  ) -> Result<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Error> {
    self
      .resolver
      .resolve(addr)
      .await
      .map_err(|e| Self::Error::Resolve {
        addr: addr.cheap_clone(),
        err: e,
      })
  }

  #[inline]
  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  #[inline]
  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.local_addr
  }

  #[inline]
  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  #[inline]
  fn max_packet_size(&self) -> usize {
    self.max_packet_size
  }

  #[inline]
  fn header_overhead(&self) -> usize {
    1
  }

  fn blocked_address(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
  ) -> Result<(), Self::Error> {
    let ip = addr.ip();
    if self.opts.cidrs_policy.is_blocked(&ip) {
      Err(Self::Error::BlockedIp(ip))
    } else {
      Ok(())
    }
  }

  async fn read(
    &self,
    from: &Self::ResolvedAddress,
    conn: &mut <Self::Connection as Connection>::Reader,
  ) -> Result<usize, Self::Error> {
    let mut buf = [0; 1];
    conn.read_exact(&mut buf).await?;
    match StreamType::try_from(buf[0]) {
      Ok(StreamType::Stream) => Ok(1),
      Ok(StreamType::Packet) => Err(QuicTransportError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("receive an unexpected packet stream from {from}"),
      ))),
      Err(tag) => Err(QuicTransportError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
          "receive a stream from {from} with invalid type value: {}",
          tag
        ),
      ))),
    }
  }

  async fn write(
    &self,
    conn: &mut <Self::Connection as Connection>::Writer,
    mut src: Payload,
  ) -> Result<usize, Self::Error> {
    use memberlist_core::proto::ProtoWriter;

    let header = src.header_mut();
    if header.is_empty() {
      return Err(QuicTransportError::custom(
        "not enough space for header".into(),
      ));
    }
    header[0] = StreamType::Stream as u8;
    let ttl = self
      .opts
      .timeout
      .map(|ttl| <Self::Runtime as RuntimeLite>::now() + ttl);

    let src = src.as_slice();
    tracing::trace!(
      total_bytes = %src.len(),
      sent = ?src,
      "memberlist_quic.stream"
    );

    match ttl {
      None => {
        conn.write_all(src).await?;
        conn.close().await?;

        Ok(src.len())
      }
      Some(ttl) => R::timeout_at(ttl, async {
        conn.write_all(src).await?;
        conn.close().await.map(|_| src.len())
      })
      .await
      .map_err(std::io::Error::from)?
      .map_err(Into::into),
    }
  }

  async fn send_to(
    &self,
    addr: &Self::ResolvedAddress,
    mut src: Payload,
  ) -> Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error> {
    let start = <Self::Runtime as RuntimeLite>::now();
    let ttl = self.opts.timeout.map(|ttl| start + ttl);
    let mut stream = self.fetch_stream(*addr, ttl).await?;
    let header = src.header_mut();
    if header.is_empty() {
      return Err(QuicTransportError::custom(
        "not enough space for header".into(),
      ));
    }
    header[0] = StreamType::Packet as u8;

    let src = src.as_slice();
    tracing::trace!(
      total_bytes = %src.len(),
      sent = ?src,
      "memberlist_quic.packet"
    );

    match ttl {
      None => {
        stream.write_all(src).await?;
        stream.close().await?;

        Ok((src.len(), start))
      }
      Some(ttl) => R::timeout_at(ttl, async {
        stream.write_all(src).await?;
        stream.close().await.map(|_| (src.len(), start))
      })
      .await
      .map_err(std::io::Error::from)?
      .map_err(Into::into),
    }
  }

  async fn open_bi(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    deadline: R::Instant,
  ) -> Result<Self::Connection, Self::Error> {
    self.fetch_stream(*addr, Some(deadline)).await
  }

  async fn open_uni(
    &self,
    addr: &Self::ResolvedAddress,
    deadline: <Self::Runtime as RuntimeLite>::Instant,
  ) -> Result<<Self::Connection as Connection>::Writer, Self::Error> {
    self.fetch_send_stream(*addr, Some(deadline)).await
  }

  fn packet(
    &self,
  ) -> PacketSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, R::Instant> {
    self.packet_rx.clone()
  }

  fn stream(
    &self,
  ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Connection> {
    self.stream_rx.clone()
  }

  #[inline]
  fn packet_reliable(&self) -> bool {
    true
  }

  #[inline]
  fn packet_secure(&self) -> bool {
    true
  }

  #[inline]
  fn stream_secure(&self) -> bool {
    true
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    if !self.shutdown_tx.close() {
      return Ok(());
    }

    for conn in self.connection_pool.iter() {
      let (_, conn) = conn.value();
      let addr = conn.local_addr();
      if let Err(e) = conn.close().await {
        tracing::error!(err = %e, local_addr=%addr, "memberlist.transport.quic: failed to close connection");
      }
    }

    for connector in self.v4_connectors.iter().chain(self.v6_connectors.iter()) {
      let addr = connector.local_addr();
      if let Err(e) = connector.close().await.map_err(Self::Error::from) {
        tracing::error!(err = %e, local_addr=%addr, "memberlist.transport.quic: failed to close connector");
      }
    }

    // Block until all the listener threads have died.
    let mut handles = core::mem::take(&mut *self.handles.borrow_mut());
    while handles.next().await.is_some() {}
    Ok(())
  }
}

impl<I, A, S, R> Drop for QuicTransport<I, A, S, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer<Runtime = R>,
  R: RuntimeLite,
{
  fn drop(&mut self) {
    self.shutdown_tx.close();
  }
}
