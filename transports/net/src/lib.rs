//! [`Transport`](memberlist_core::Transport)'s network transport layer based on TCP and UDP.
#![allow(clippy::type_complexity)]
#![deny(missing_docs)]
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
  time::{Duration, Instant},
};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime, WaitableSpawner,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, BytesMut};
use checksum::CHECKSUM_SIZE;
use futures::{io::BufReader, AsyncRead, AsyncWrite, AsyncWriteExt};
use memberlist_core::{
  transport::{
    resolver::AddressResolver,
    stream::{packet_stream, promised_stream, PacketSubscriber, StreamSubscriber},
    Id, Transport, Wire,
  },
  types::{Message, SmallVec, TinyVec},
  CheapClone,
};
use memberlist_utils::{net::IsGlobalIp, *};
use peekable::future::AsyncPeekExt;

#[doc(inline)]
pub use memberlist_utils as utils;

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

/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`NetTransport`](crate::NetTransport).
pub mod stream_layer;
use stream_layer::*;

mod label;
pub use label::Label;

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

/// The net transport based on TCP/TLS and UDP
pub struct NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  opts: Arc<NetTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  local_addr: A::Address,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  v4_round_robin: AtomicUsize,
  v4_sockets: SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>,
  v6_round_robin: AtomicUsize,
  v6_sockets: SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>,
  stream_layer: Arc<S>,
  #[cfg(feature = "encryption")]
  encryptor: Option<SecretKeyring>,
  wg: WaitableSpawner<A::Runtime>,
  resolver: Arc<A>,
  shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,
  _marker: PhantomData<W>,
}

impl<I, A, S, W> NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  /// Creates a new net transport.
  pub async fn new(
    resolver: A,
    stream_layer: S,
    opts: NetTransportOptions<I, A>,
  ) -> Result<Self, NetTransportError<A, W>> {
    let resolver = Arc::new(resolver);
    let stream_layer = Arc::new(stream_layer);
    let opts = Arc::new(opts);
    #[cfg(feature = "encryption")]
    let keyring = match (opts.primary_key, &opts.secret_keys) {
      (None, Some(keys)) if !keys.is_empty() => {
        tracing::warn!(
          target = "memberlist",
          "using first key in keyring as primary key"
        );
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
    opts: Arc<NetTransportOptions<I, A>>,
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
            Ok(ln) => break (ln.local_addr().unwrap(), ln),
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
          Ok(ln) => (ln.local_addr().unwrap(), ln),
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
    let wg = WaitableSpawner::new();
    let shutdown = Arc::new(AtomicBool::new(false));

    // Fire them up start that we've been able to create them all.
    // keep the first tcp and udp listener, gossip protocol, we made sure there's at least one
    // udp and tcp listener can
    for ((promised_ln, promised_addr), (socket, socket_addr)) in v4_promised_listeners
      .iter()
      .zip(v4_sockets.iter())
      .chain(v6_promised_listeners.iter().zip(v6_sockets.iter()))
    {
      wg.spawn_detach(
        PromisedProcessor::<A, Self, S> {
          stream_tx: stream_tx.clone(),
          ln: promised_ln.clone(),
          shutdown: shutdown.clone(),
          shutdown_rx: shutdown_rx.clone(),
          local_addr: *promised_addr,
        }
        .run(),
      );

      wg.spawn_detach(
        PacketProcessor::<A, Self> {
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
        }
        .run(),
      );
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
        tracing::warn!(target = "memberlist", advertise_addr=%final_advertise_addr, "binding to public address without enabling encryption for packet stream layer!");
      }

      #[cfg(feature = "encryption")]
      if !S::is_secure()
        && (encryptor.is_none() || opts.encryption_algo.is_none() || !opts.gossip_verify_outgoing)
      {
        tracing::warn!(target = "memberlist", advertise_addr=%final_advertise_addr, "binding to public address without enabling encryption for stream layer!");
      }

      #[cfg(not(feature = "encryption"))]
      tracing::warn!(target = "memberlist", advertise_addr=%final_advertise_addr, "binding to public address without enabling encryption for stream layer!");
    }

    Ok(Self {
      advertise_addr: final_advertise_addr,
      local_addr: self_addr,
      opts,
      packet_rx,
      stream_rx,
      wg,
      shutdown,
      v4_sockets: v4_sockets.into_iter().map(|(ln, _)| ln).collect(),
      v4_round_robin: AtomicUsize::new(0),
      v6_sockets: v6_sockets.into_iter().map(|(ln, _)| ln).collect(),
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
  ) -> &<<A::Runtime as Runtime>::Net as Net>::UdpSocket {
    if addr.is_ipv4() {
      // if there's no v4 sockets, we assume remote addr can accept both v4 and v6
      // give a try on v6
      if self.v4_sockets.is_empty() {
        let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.v6_sockets.len();
        &self.v6_sockets[idx]
      } else {
        let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.v4_sockets.len();
        &self.v4_sockets[idx]
      }
    } else if self.v6_sockets.is_empty() {
      let idx = self.v4_round_robin.fetch_add(1, Ordering::AcqRel) % self.v4_sockets.len();
      &self.v4_sockets[idx]
    } else {
      let idx = self.v6_round_robin.fetch_add(1, Ordering::AcqRel) % self.v6_sockets.len();
      &self.v6_sockets[idx]
    }
  }
}

struct Batch<I, A> {
  num_packets: usize,
  packets: TinyVec<Message<I, A>>,
  estimate_encoded_len: usize,
}

impl<I, A> Batch<I, A> {
  fn estimate_encoded_len(&self) -> usize {
    if self.packets.len() == 1 {
      return self.estimate_encoded_len - PACKET_HEADER_OVERHEAD - PACKET_OVERHEAD;
    }
    self.estimate_encoded_len
  }
}

impl<I, A, S, W> Transport for NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  type Error = NetTransportError<Self::Resolver, Self::Wire>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

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
    let mut conn = BufReader::new(conn).peekable();
    let mut stream_label = label::remove_label_header(&mut conn).await.map_err(|e| {
      tracing::error!(target = "memberlist.net.promised", remote = %from, err=%e, "failed to receive and remove the stream label header");
      ConnectionError::promised_read(e)
    })?.unwrap_or_else(Label::empty);

    let label = &self.opts.label;

    if self.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(
          target = "memberlist.net.promised",
          "unexpected double stream label header"
        );
        return Err(LabelError::duplicate(label.cheap_clone(), stream_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      stream_label = label.cheap_clone();
    }

    if stream_label.ne(&self.opts.label) {
      tracing::error!(target = "memberlist.net.promised", local_label=%label, remote_label=%stream_label, "discarding stream with unacceptable label");
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
    self.send_by_promised(conn, msg).await
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, Instant), Self::Error> {
    let start = Instant::now();
    let encoded_size = W::encoded_len(&packet);
    self
      .send_batch(
        addr,
        Batch {
          packets: TinyVec::from(packet),
          num_packets: 1,
          estimate_encoded_len: self.packets_header_overhead() + PACKET_OVERHEAD + encoded_size,
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

    let mut batches =
      SmallVec::<Batch<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>::new();
    let packets_overhead = self.packets_header_overhead();
    let mut estimate_batch_encoded_size = packets_overhead;
    let mut current_packets_in_batch = 0;

    // get how many packets a batch
    for packet in packets.iter() {
      let ep_len = W::encoded_len(packet);
      // check if we reach the maximum packet size
      let current_encoded_size = estimate_batch_encoded_size + PACKET_OVERHEAD + ep_len;
      if current_encoded_size >= self.max_payload_size()
        || current_packets_in_batch >= NUM_PACKETS_PER_BATCH
      {
        batches.push(Batch {
          packets: TinyVec::with_capacity(current_packets_in_batch),
          num_packets: current_packets_in_batch,
          estimate_encoded_len: estimate_batch_encoded_size,
        });
        estimate_batch_encoded_size =
          packets_overhead + PACKET_HEADER_OVERHEAD + PACKET_OVERHEAD + ep_len;
        current_packets_in_batch = 1;
      } else {
        estimate_batch_encoded_size += PACKET_OVERHEAD + ep_len;
        current_packets_in_batch += 1;
      }
    }

    // consume the packets to small batches according to batch_offsets.

    // if batch_offsets is empty, means that packets can be sent by one I/O call
    if batches.is_empty() {
      self
        .send_batch(
          addr,
          Batch {
            num_packets: packets.len(),
            packets,
            estimate_encoded_len: estimate_batch_encoded_size,
          },
        )
        .await
        .map(|sent| (sent, start))
    } else {
      let mut batch_idx = 0;
      for (idx, packet) in packets.into_iter().enumerate() {
        let batch = &mut batches[batch_idx];
        batch.packets.push(packet);
        if batch.num_packets == idx - 1 {
          batch_idx += 1;
        }
      }

      let mut total_bytes_sent = 0;
      let resps =
        futures::future::join_all(batches.into_iter().map(|b| self.send_batch(addr, b))).await;

      for res in resps {
        match res {
          Ok(sent) => {
            total_bytes_sent += sent;
          }
          Err(e) => return Err(e),
        }
      }
      Ok((total_bytes_sent, start))
    }
  }

  async fn dial_timeout(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    timeout: Duration,
  ) -> Result<Self::Stream, Self::Error> {
    let connector = <Self::Runtime as Runtime>::timeout(timeout, self.stream_layer.connect(*addr));
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
    self.stream_layer.cache_stream(*addr, stream);
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
    if self.shutdown_tx.is_closed() {
      return Ok(());
    }

    // This will avoid log spam about errors when we shut down.
    self.shutdown.store(true, Ordering::SeqCst);
    self.shutdown_tx.close();

    // Block until all the listener threads have died.
    self.wg.wait().await;
    Ok(())
  }
}

impl<I, A, S, W> Drop for NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  fn drop(&mut self) {
    use pollster::FutureExt as _;

    if self.shutdown_tx.is_closed() {
      return;
    }

    let _ = self.shutdown().block_on();
  }
}
