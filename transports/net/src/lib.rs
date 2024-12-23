//! [`memberlist`](https://crates.io/crates/memberlist)'s [`Transport`] layer based on TCP and UDP.
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![allow(clippy::type_complexity)]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  io::{Error, ErrorKind},
  marker::PhantomData,
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::Instant,
};

use agnostic::{
  net::{Net, UdpSocket},
  AsyncSpawner, Runtime, RuntimeLite,
};
use atomic_refcell::AtomicRefCell;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut};
use checksum::CHECKSUM_SIZE;
use futures::{
  io::BufReader, stream::FuturesUnordered, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
  StreamExt,
};
pub use memberlist_core::{
  transport::*,
  types::{CIDRsPolicy, Label, LabelError},
};
use memberlist_core::{
  types::{Message, SmallVec, TinyVec},
  util::{batch, Batch, IsGlobalIp},
};
use peekable::future::{AsyncPeekExt, AsyncPeekable};

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;
#[cfg(feature = "compression")]
use compressor::*;

mod io;

mod options;
pub use options::*;

mod promised_processor;
use promised_processor::*;
mod packet_processor;
use packet_processor::*;

/// Encrypt/decrypt related.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub mod security;
#[cfg(feature = "encryption")]
use security::{EncryptionAlgo, SecretKey, SecretKeyring, SecurityError};

/// Errors for the net transport.
pub mod error;
use error::*;

/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`NetTransport`].
pub mod stream_layer;
use stream_layer::*;

mod label;

mod checksum;
pub use checksum::Checksumer;

/// Re-exports [`nodecraft`]'s address resolver.
pub mod resolver {
  #[cfg(feature = "dns")]
  pub use nodecraft::resolver::dns;
  pub use nodecraft::resolver::{address, socket_addr};
}

/// Exports unit tests.
#[cfg(any(test, feature = "test"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test")))]
pub mod tests;

const CHECKSUM_TAG: core::ops::RangeInclusive<u8> = 44..=64;
#[cfg(feature = "encryption")]
const ENCRYPT_TAG: core::ops::RangeInclusive<u8> = 65..=85;
#[cfg(feature = "compression")]
const COMPRESS_TAG: core::ops::RangeInclusive<u8> = 86..=126;

#[cfg(feature = "compression")]
const COMPRESS_HEADER: usize = 1 + core::mem::size_of::<u32>();
#[cfg(feature = "encryption")]
const ENCRYPT_HEADER: usize = 1 + core::mem::size_of::<u32>();
const CHECKSUM_HEADER: usize = 1 + CHECKSUM_SIZE;

#[cfg(any(feature = "compression", feature = "encryption"))]
const MAX_MESSAGE_LEN_SIZE: usize = core::mem::size_of::<u32>();

const MAX_PACKET_SIZE: usize = u16::MAX as usize;
/// max message bytes is `u16::MAX`
const PACKET_OVERHEAD: usize = core::mem::size_of::<u16>();
/// tag + num msgs (max number of messages is `255`)
const PACKET_HEADER_OVERHEAD: usize = 1 + 1;
const NUM_PACKETS_PER_BATCH: usize = 255;

/// A large buffer size that we attempt to set UDP
/// sockets to in order to handle a large volume of messages.
const PACKET_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

#[cfg(feature = "tokio")]
/// [`NetTransport`] based on [`tokio`](https://crates.io/crates/tokio).
pub type TokioNetTransport<I, A, S, W> = NetTransport<I, A, S, W, agnostic::tokio::TokioRuntime>;

#[cfg(feature = "async-std")]
/// [`NetTransport`] based on [`async-std`](https://crates.io/crates/async-std).
pub type AsyncStdNetTransport<I, A, S, W> =
  NetTransport<I, A, S, W, agnostic::async_std::AsyncStdRuntime>;

#[cfg(feature = "smol")]
/// [`NetTransport`] based on [`smol`](https://crates.io/crates/smol).
pub type SmolNetTransport<I, A, S, W> = NetTransport<I, A, S, W, agnostic::smol::SmolRuntime>;

/// The net transport based on TCP/TLS and UDP
pub struct NetTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  opts: Arc<Options<I, A>>,
  advertise_addr: A::ResolvedAddress,
  local_addr: A::Address,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  num_v4_sockets: usize,
  v4_round_robin: AtomicUsize,
  v4_sockets: AtomicRefCell<SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>>,
  num_v6_sockets: usize,
  v6_round_robin: AtomicUsize,
  v6_sockets: AtomicRefCell<SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>>,
  stream_layer: Arc<S>,
  #[cfg(feature = "encryption")]
  encryptor: Option<SecretKeyring>,
  handles: AtomicRefCell<FuturesUnordered<<R::Spawner as AsyncSpawner>::JoinHandle<()>>>,
  resolver: Arc<A>,
  shutdown_tx: async_channel::Sender<()>,
  _marker: PhantomData<W>,
}

impl<I, A, S, W, R> NetTransport<I, A, S, W, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  /// Creates a new net transport.
  async fn _new(opts: NetTransportOptions<I, A, S>) -> Result<Self, NetTransportError<A, W>> {
    let (resolver_opts, stream_layer_opts, opts) = opts.into();
    let resolver = Arc::new(
      <A as AddressResolver>::new(resolver_opts)
        .await
        .map_err(NetTransportError::Resolver)?,
    );

    let stream_layer = Arc::new(
      <S as StreamLayer>::new(stream_layer_opts)
        .await
        .map_err(NetTransportError::StreamLayer)?,
    );
    let opts = Arc::new(opts);
    #[cfg(feature = "encryption")]
    let keyring = match (opts.primary_key, &opts.secret_keys) {
      (None, Some(keys)) if !keys.is_empty() => {
        tracing::warn!("memberlist: using first key in keyring as primary key");
        let mut iter = keys.iter().copied();
        let pk = iter.next().unwrap();
        let keyring = SecretKeyring::with_keys(pk, iter);
        Some(keyring)
      }
      (Some(pk), None) => Some(SecretKeyring::new(pk)),
      (Some(pk), Some(keys)) => Some(SecretKeyring::with_keys(pk, keys.iter().copied())),
      _ => None,
    };

    Self::new_in(
      resolver.clone(),
      stream_layer.clone(),
      opts.clone(),
      #[cfg(feature = "encryption")]
      keyring.clone(),
    )
    .await
  }

  fn find_advertise_addr_index(addrs: &[SocketAddr]) -> usize {
    for (i, addr) in addrs.iter().enumerate() {
      if !addr.ip().is_unspecified() {
        return i;
      }
    }

    0
  }

  async fn new_in(
    resolver: Arc<A>,
    stream_layer: Arc<S>,
    opts: Arc<Options<I, A>>,
    #[cfg(feature = "encryption")] encryptor: Option<SecretKeyring>,
  ) -> Result<Self, NetTransportError<A, W>> {
    // If we reject the empty list outright we can assume that there's at
    // least one listener of each type later during operation.
    if opts.bind_addresses.is_empty() {
      return Err(NetTransportError::EmptyBindAddresses);
    }

    let (stream_tx, stream_rx) = promised_stream::<Self>();
    let (packet_tx, packet_rx) = packet_stream::<Self>();
    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let mut v4_promised_listeners = Vec::with_capacity(opts.bind_addresses.len());
    let mut v4_sockets = Vec::with_capacity(opts.bind_addresses.len());
    let mut v6_promised_listeners = Vec::with_capacity(opts.bind_addresses.len());
    let mut v6_sockets = Vec::with_capacity(opts.bind_addresses.len());
    let mut resolved_bind_address = SmallVec::new();

    for addr in opts.bind_addresses.iter() {
      let addr = resolver
        .resolve(addr)
        .await
        .map_err(|e| NetTransportError::Resolve {
          addr: addr.clone(),
          err: e,
        })?;
      let bind_port = addr.port();

      let (local_addr, ln) = if bind_port == 0 {
        let mut retries = 0;
        loop {
          match stream_layer.bind(addr).await {
            Ok(ln) => break (ln.local_addr(), ln),
            Err(e) => {
              if retries < 9 {
                retries += 1;
                continue;
              }
              return Err(NetTransportError::ListenPromised(addr, e));
            }
          }
        }
      } else {
        match stream_layer.bind(addr).await {
          Ok(ln) => (ln.local_addr(), ln),
          Err(e) => return Err(NetTransportError::ListenPromised(addr, e)),
        }
      };

      if local_addr.is_ipv4() {
        v4_promised_listeners.push((Arc::new(ln), local_addr));
      } else {
        v6_promised_listeners.push((Arc::new(ln), local_addr));
      }
      // If the config port given was zero, use the first TCP listener
      // to pick an available port and then apply that to everything
      // else.
      let addr = if bind_port == 0 { local_addr } else { addr };
      resolved_bind_address.push(addr);

      let (local_addr, packet_socket) =
        <<<A::Runtime as Runtime>::Net as Net>::UdpSocket as UdpSocket>::bind(addr)
          .await
          .map(|ln| (addr, ln))
          .map_err(|e| NetTransportError::ListenPacket(addr, e))?;
      if local_addr.is_ipv4() {
        v4_sockets.push((Arc::new(packet_socket), local_addr));
      } else {
        v6_sockets.push((Arc::new(packet_socket), local_addr))
      }
    }

    let expose_addr_index = Self::find_advertise_addr_index(&resolved_bind_address);
    let advertise_addr = resolved_bind_address[expose_addr_index];
    let self_addr = opts.bind_addresses[expose_addr_index].cheap_clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let handles = FuturesUnordered::new();
    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for ((promised_ln, promised_addr), (socket, socket_addr)) in v4_promised_listeners
      .iter()
      .zip(v4_sockets.iter())
      .chain(v6_promised_listeners.iter().zip(v6_sockets.iter()))
    {
      let processor = PromisedProcessor::<A, Self, S> {
        stream_tx: stream_tx.clone(),
        ln: promised_ln.clone(),
        shutdown_rx: shutdown_rx.clone(),
        local_addr: *promised_addr,
      };
      handles.push(R::spawn(processor.run()));

      let processor = PacketProcessor::<A, Self> {
        packet_tx: packet_tx.clone(),
        label: opts.label.clone(),
        #[cfg(any(feature = "compression", feature = "encryption"))]
        offload_size: opts.offload_size,
        #[cfg(feature = "encryption")]
        verify_incoming: opts.gossip_verify_incoming,
        #[cfg(feature = "encryption")]
        encryptor: encryptor.clone(),
        socket: socket.clone(),
        local_addr: *socket_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
        shutdown_rx: shutdown_rx.clone(),
        skip_inbound_label_check: opts.skip_inbound_label_check,
      };

      handles.push(R::spawn(processor.run()));
    }

    // find final advertise address
    let final_advertise_addr = if advertise_addr.ip().is_unspecified() {
      let ip = local_ip_address::local_ip().map_err(|e| match e {
        local_ip_address::Error::LocalIpAddressNotFound => NetTransportError::NoPrivateIP,
        e => NetTransportError::NoInterfaceAddresses(e),
      })?;
      SocketAddr::new(ip, advertise_addr.port())
    } else {
      advertise_addr
    };

    if final_advertise_addr.is_global_ip() {
      #[cfg(feature = "encryption")]
      if S::is_secure()
        && (encryptor.is_none() || opts.encryption_algo.is_none() || !opts.gossip_verify_outgoing)
      {
        tracing::warn!(advertise_addr=%final_advertise_addr, "memberlist_net: binding to public address without enabling encryption for packet stream layer!");
      }

      #[cfg(feature = "encryption")]
      if !S::is_secure()
        && (encryptor.is_none() || opts.encryption_algo.is_none() || !opts.gossip_verify_outgoing)
      {
        tracing::warn!(advertise_addr=%final_advertise_addr, "memberlist_net: binding to public address without enabling encryption for stream layer!");
      }

      #[cfg(not(feature = "encryption"))]
      tracing::warn!(advertise_addr=%final_advertise_addr, "memberlist_net: binding to public address without enabling encryption for stream layer!");
    }

    Ok(Self {
      advertise_addr: final_advertise_addr,
      local_addr: self_addr,
      opts,
      packet_rx,
      stream_rx,
      handles: AtomicRefCell::new(handles),
      num_v4_sockets: v4_sockets.len(),
      v4_sockets: AtomicRefCell::new(v4_sockets.into_iter().map(|(ln, _)| ln).collect()),
      v4_round_robin: AtomicUsize::new(0),
      num_v6_sockets: v6_sockets.len(),
      v6_sockets: AtomicRefCell::new(v6_sockets.into_iter().map(|(ln, _)| ln).collect()),
      v6_round_robin: AtomicUsize::new(0),
      stream_layer,
      #[cfg(feature = "encryption")]
      encryptor,
      resolver,
      shutdown_tx,
      _marker: PhantomData,
    })
  }

  fn next_socket(
    &self,
    addr: &A::ResolvedAddress,
  ) -> Option<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>> {
    enum Kind {
      V4(usize),
      V6(usize),
    }

    let kind = if addr.is_ipv4() {
      // if there's no v4 sockets, we assume remote addr can accept both v4 and v6
      // give a try on v6
      if self.num_v4_sockets == 0 {
        let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.num_v6_sockets;
        Kind::V6(idx)
      } else {
        let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.num_v4_sockets;
        Kind::V4(idx)
      }
    } else if self.num_v6_sockets == 0 {
      let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.num_v4_sockets;
      Kind::V4(idx)
    } else {
      let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.num_v6_sockets;
      Kind::V6(idx)
    };

    // if we failed to borrow, it means that this transport is being shut down.

    match kind {
      Kind::V4(idx) => {
        if let Ok(sockets) = self.v4_sockets.try_borrow() {
          Some(sockets[idx].clone())
        } else {
          None
        }
      }
      Kind::V6(idx) => {
        if let Ok(sockets) = self.v6_sockets.try_borrow() {
          Some(sockets[idx].clone())
        } else {
          None
        }
      }
    }
  }
}

impl<I, A, S, W, R> Transport for NetTransport<I, A, S, W, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  type Error = NetTransportError<Self::Resolver, Self::Wire>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

  type Options = NetTransportOptions<Self::Id, Self::Resolver, S>;

  async fn new(transport_opts: Self::Options) -> Result<Self, Self::Error> {
    Self::_new(transport_opts).await
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

  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.local_addr
  }

  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  #[cfg(feature = "encryption")]
  fn keyring(&self) -> Option<&SecretKeyring> {
    self.encryptor.as_ref()
  }

  #[cfg(feature = "encryption")]
  fn encryption_enabled(&self) -> bool {
    self.encryptor.is_some()
      && self.opts.encryption_algo.is_some()
      && self.opts.gossip_verify_outgoing
  }

  fn max_payload_size(&self) -> usize {
    MAX_PACKET_SIZE.min(self.opts.max_payload_size)
  }

  fn packet_overhead(&self) -> usize {
    PACKET_OVERHEAD
  }

  fn packets_header_overhead(&self) -> usize {
    self.fix_packet_overhead() + PACKET_HEADER_OVERHEAD
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

  async fn read_message(
    &self,
    from: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    conn: &mut Self::Stream,
  ) -> Result<
    (
      usize,
      Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Self::Error,
  > {
    let ddl = conn.read_deadline();
    let mut conn = BufReader::new(conn).peekable().with_deadline(ddl);
    let mut stream_label = label::remove_label_header::<R>(&mut conn).await.map_err(|e| {
      tracing::error!(remote = %from, err=%e, "memberlist_net.promised: failed to receive and remove the stream label header");
      ConnectionError::promised_read(e)
    })?.unwrap_or_else(Label::empty);

    let label = &self.opts.label;

    if self.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!("memberlist_net.promised: unexpected double stream label header");
        return Err(LabelError::duplicate(label.cheap_clone(), stream_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      stream_label = label.cheap_clone();
    }

    if stream_label.ne(&self.opts.label) {
      tracing::error!(local_label=%label, remote_label=%stream_label, "memberlist_net.promised: discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), stream_label).into());
    }

    let readed = stream_label.encoded_overhead();

    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    return self
      .read_from_promised_without_compression_and_encryption(conn)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    return self
      .read_from_promised_with_compression_without_encryption(conn)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    return self
      .read_from_promised_with_encryption_without_compression(conn, stream_label, from)
      .await
      .map(|(read, msg)| (readed + read, msg));

    #[cfg(all(feature = "compression", feature = "encryption"))]
    self
      .read_from_promised_with_compression_and_encryption(conn, stream_label, from)
      .await
      .map(|(read, msg)| (readed + read, msg))
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    let ddl = conn.write_deadline();
    self.send_by_promised(conn.with_deadline(ddl), msg).await
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, Instant), Self::Error> {
    let start = Instant::now();
    let encoded_size = W::encoded_len(&packet);
    let packets_overhead = self.packets_header_overhead();
    self
      .send_batch(
        addr,
        Batch::One {
          msg: packet,
          estimate_encoded_size: packets_overhead - PACKET_HEADER_OVERHEAD + encoded_size,
        },
      )
      .await
      .map(|sent| (sent, start))
  }

  async fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<(usize, Instant), Self::Error> {
    let start = Instant::now();

    let packets_overhead = self.packets_header_overhead();
    let batches = batch::<_, _, _, Self::Wire>(
      packets_overhead - PACKET_HEADER_OVERHEAD,
      PACKET_HEADER_OVERHEAD,
      PACKET_OVERHEAD,
      self.max_payload_size(),
      u16::MAX as usize,
      NUM_PACKETS_PER_BATCH,
      packets,
    );

    let mut total_bytes_sent = 0;
    let mut futs = batches
      .into_iter()
      .map(|b| self.send_batch(addr, b))
      .collect::<FuturesUnordered<_>>();
    while let Some(res) = futs.next().await {
      match res {
        Ok(sent) => {
          total_bytes_sent += sent;
        }
        Err(e) => return Err(e),
      }
    }
    Ok((total_bytes_sent, start))
  }

  async fn dial_with_deadline(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    deadline: Instant,
  ) -> Result<Self::Stream, Self::Error> {
    let connector =
      <Self::Runtime as RuntimeLite>::timeout_at(deadline, self.stream_layer.connect(*addr));
    match connector.await {
      Ok(Ok(conn)) => Ok(conn),
      Ok(Err(e)) => Err(Self::Error::Connection(ConnectionError {
        kind: ConnectionKind::Promised,
        error_kind: ConnectionErrorKind::Dial,
        error: e,
      })),
      Err(_) => Err(NetTransportError::Connection(ConnectionError {
        kind: ConnectionKind::Promised,
        error_kind: ConnectionErrorKind::Dial,
        error: Error::new(ErrorKind::TimedOut, "timeout"),
      })),
    }
  }

  async fn cache_stream(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    stream: Self::Stream,
  ) -> Result<(), Self::Error> {
    self.stream_layer.cache_stream(*addr, stream).await;
    Ok(())
  }

  fn packet(
    &self,
  ) -> PacketSubscriber<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress> {
    self.packet_rx.clone()
  }

  fn stream(
    &self,
  ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Stream> {
    self.stream_rx.clone()
  }

  async fn shutdown(&self) -> Result<(), Self::Error> {
    if !self.shutdown_tx.close() {
      return Ok(());
    }

    // clear all udp sockets
    loop {
      if let Ok(mut s) = self.v4_sockets.try_borrow_mut() {
        s.clear();
        break;
      }
    }

    loop {
      if let Ok(mut s) = self.v6_sockets.try_borrow_mut() {
        s.clear();
        break;
      }
    }

    let mut handles = core::mem::take(&mut *self.handles.borrow_mut());
    while handles.next().await.is_some() {}
    Ok(())
  }
}

impl<I, A, S, W, R> Drop for NetTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  fn drop(&mut self) {
    self.shutdown_tx.close();
  }
}

trait WithDeadline: Sized {
  fn with_deadline(self, deadline: Option<Instant>) -> Deadline<Self> {
    Deadline { op: self, deadline }
  }
}

impl<T> WithDeadline for T {}

struct Deadline<T> {
  op: T,
  deadline: Option<Instant>,
}

impl<T: AsyncRead + Send + Unpin> Deadline<T> {
  async fn read_exact<R: Runtime>(&mut self, dst: &mut [u8]) -> std::io::Result<()> {
    match self.deadline {
      Some(ddl) => R::timeout_at(ddl, self.op.read_exact(dst)).await?,
      None => self.op.read_exact(dst).await,
    }
  }
}

impl<T: AsyncWrite + Send + Unpin> Deadline<T> {
  async fn write_all<R: Runtime>(&mut self, src: &[u8]) -> std::io::Result<()> {
    match self.deadline {
      Some(ddl) => R::timeout_at(ddl, self.op.write_all(src)).await?,
      None => self.op.write_all(src).await,
    }
  }
}

impl<T: AsyncRead + Send + Unpin> Deadline<AsyncPeekable<T>> {
  async fn peek_exact<R: Runtime>(&mut self, dst: &mut [u8]) -> std::io::Result<()> {
    match self.deadline {
      Some(ddl) => R::timeout_at(ddl, self.op.peek_exact(dst)).await?,
      None => self.op.peek_exact(dst).await,
    }
  }
}
