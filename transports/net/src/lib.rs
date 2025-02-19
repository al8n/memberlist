//! [`memberlist`](https://crates.io/crates/memberlist)'s [`Transport`] layer based on TCP and UDP.
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![allow(clippy::type_complexity)]
#![deny(missing_docs, warnings)]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use std::{
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
};

use agnostic::{
  net::{Net, UdpSocket},
  AsyncSpawner, Runtime, RuntimeLite,
};
use atomic_refcell::AtomicRefCell;
use futures::{stream::FuturesUnordered, StreamExt};
use memberlist_core::types::{Data, Payload, SmallVec};
pub use memberlist_core::{
  transport::*,
  types::{CIDRsPolicy, Label, LabelError},
};

mod options;
pub use options::*;

mod promised_processor;
use promised_processor::*;
mod packet_processor;
use packet_processor::*;

/// Errors for the net transport.
pub mod error;
use error::*;

/// Abstract the [`StremLayer`](crate::stream_layer::StreamLayer) for [`NetTransport`].
pub mod stream_layer;
use stream_layer::*;

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

/// A large buffer size that we attempt to set UDP
/// sockets to in order to handle a large volume of messages.
const DEFAULT_UDP_RECV_BUF_SIZE: usize = 2 * 1024 * 1024;

#[cfg(feature = "tokio")]
/// [`NetTransport`] based on [`tokio`](https://crates.io/crates/tokio).
pub type TokioNetTransport<I, A, S> = NetTransport<I, A, S, agnostic::tokio::TokioRuntime>;

#[cfg(feature = "async-std")]
/// [`NetTransport`] based on [`async-std`](https://crates.io/crates/async-std).
pub type AsyncStdNetTransport<I, A, S> =
  NetTransport<I, A, S, agnostic::async_std::AsyncStdRuntime>;

#[cfg(feature = "smol")]
/// [`NetTransport`] based on [`smol`](https://crates.io/crates/smol).
pub type SmolNetTransport<I, A, S> = NetTransport<I, A, S, agnostic::smol::SmolRuntime>;

/// The net transport based on TCP/TLS and UDP
pub struct NetTransport<I, A, S, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  opts: Arc<Options<I, A>>,
  advertise_addr: A::ResolvedAddress,
  local_addr: A::Address,
  packet_rx: PacketSubscriber<A::ResolvedAddress, R::Instant>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  num_v4_sockets: usize,
  v4_round_robin: AtomicUsize,
  v4_sockets: AtomicRefCell<SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>>,
  num_v6_sockets: usize,
  v6_round_robin: AtomicUsize,
  v6_sockets: AtomicRefCell<SmallVec<Arc<<<A::Runtime as Runtime>::Net as Net>::UdpSocket>>>,
  stream_layer: Arc<S>,
  handles: AtomicRefCell<FuturesUnordered<<R::Spawner as AsyncSpawner>::JoinHandle<()>>>,
  resolver: Arc<A>,
  shutdown_tx: async_channel::Sender<()>,
}

impl<I, A, S, R> NetTransport<I, A, S, R>
where
  I: Id + Data + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  A::ResolvedAddress: Data,
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  fn find_advertise_addr_index(addrs: &[SocketAddr]) -> usize {
    for (i, addr) in addrs.iter().enumerate() {
      if !addr.ip().is_unspecified() {
        return i;
      }
    }

    0
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

impl<I, A, S, R> Transport for NetTransport<I, A, S, R>
where
  I: Id + Data + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Data + Send + Sync + 'static,
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  type Error = NetTransportError<Self::Resolver>;

  type Id = I;
  type Address = A::Address;
  type ResolvedAddress = SocketAddr;
  type Resolver = A;

  type Stream = S::Stream;

  type Runtime = <Self::Resolver as AddressResolver>::Runtime;

  type Options = NetTransportOptions<Self::Id, Self::Resolver, S>;

  async fn new(transport_opts: Self::Options) -> Result<Self, Self::Error> {
    let (resolver_opts, stream_layer_opts, opts) = transport_opts.into();
    let resolver = Arc::new(
      <A as AddressResolver>::new(resolver_opts)
        .await
        .map_err(NetTransportError::Resolver)?,
    );

    let stream_layer = Arc::new(<S as StreamLayer>::new(stream_layer_opts).await?);
    let opts = Arc::new(opts);

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

      set_udp_recv_buffer(&packet_socket, opts.recv_buffer_size)?;

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
        socket: socket.clone(),
        local_addr: *socket_addr,
        shutdown: shutdown.clone(),
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels.clone().unwrap_or_default(),
        shutdown_rx: shutdown_rx.clone(),
      };

      handles.push(R::spawn(processor.run()));
    }

    // find final advertise address
    let final_advertise_addr = if advertise_addr.ip().is_unspecified() {
      let ip = getifs::private_addrs()
        .map_err(|_| NetTransportError::NoPrivateIP)
        .and_then(|ips| {
          if let Some(ip) = ips.into_iter().next().map(|ip| ip.addr()) {
            Ok(ip)
          } else {
            Err(NetTransportError::NoPrivateIP)
          }
        })?;
      SocketAddr::new(ip, advertise_addr.port())
    } else {
      advertise_addr
    };

    // if final_advertise_addr.is_global_ip() {
    //   #[cfg(feature = "encryption")]
    //   if S::is_secure()
    //     && (encryptor.is_none() || opts.encryption_algo.is_none() || !opts.gossip_verify_outgoing)
    //   {
    //     tracing::warn!(advertise_addr=%final_advertise_addr, "memberlist_net: binding to public address without enabling encryption for packet stream layer!");
    //   }

    //   #[cfg(feature = "encryption")]
    //   if !S::is_secure()
    //     && (encryptor.is_none() || opts.encryption_algo.is_none() || !opts.gossip_verify_outgoing)
    //   {
    //     tracing::warn!(advertise_addr=%final_advertise_addr, "memberlist_net: binding to public address without enabling encryption for stream layer!");
    //   }

    //   #[cfg(not(feature = "encryption"))]
    //   tracing::warn!(advertise_addr=%final_advertise_addr, "memberlist_net: binding to public address without enabling encryption for stream layer!");
    // }

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
      resolver,
      shutdown_tx,
    })
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
    self.opts.max_packet_size
  }

  #[inline]
  fn header_overhead(&self) -> usize {
    0
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

  async fn send_to(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: Payload,
  ) -> Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error> {
    let start = <Self::Runtime as RuntimeLite>::now();

    let src = packets.as_slice();
    match self.next_socket(addr) {
      Some(skt) => skt
        .send_to(src, addr)
        .await
        .map(|num| {
          tracing::trace!(remote=%addr, total_bytes = %num, sent=?src, "memberlist_net.packet");
          (num, start)
        })
        .map_err(Into::into),
      None => {
        tracing::error!("memberlist_net.packet: transport is being shutdown");
        Err(
          std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "transport is being shutdown",
          )
          .into(),
        )
      }
    }
  }

  async fn dial_with_deadline(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    deadline: <Self::Runtime as RuntimeLite>::Instant,
  ) -> Result<Self::Stream, Self::Error> {
    let connector =
      <Self::Runtime as RuntimeLite>::timeout_at(deadline, self.stream_layer.connect(*addr));
    match connector.await {
      Ok(Ok(conn)) => Ok(conn),
      Ok(Err(e)) => Err(e.into()),
      Err(e) => Err(Self::Error::Io(e.into())),
    }
  }

  fn packet(
    &self,
  ) -> PacketSubscriber<
    <Self::Resolver as AddressResolver>::ResolvedAddress,
    <Self::Runtime as RuntimeLite>::Instant,
  > {
    self.packet_rx.clone()
  }

  fn stream(
    &self,
  ) -> StreamSubscriber<<Self::Resolver as AddressResolver>::ResolvedAddress, Self::Stream> {
    self.stream_rx.clone()
  }

  fn packet_reliable(&self) -> bool {
    false
  }

  fn packet_secure(&self) -> bool {
    false
  }

  fn stream_secure(&self) -> bool {
    S::is_secure()
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

impl<I, A, S, R> Drop for NetTransport<I, A, S, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer<Runtime = R>,
  R: Runtime,
{
  fn drop(&mut self) {
    self.shutdown_tx.close();
  }
}

// Resize the UDP receive window. The function
// attempts to set the read buffer to `udpRecvBuf` but backs off until
// the read buffer can be set.
fn set_udp_recv_buffer<U>(udp: &U, mut size: usize) -> std::io::Result<()>
where
  U: agnostic::net::UdpSocket,
{
  let mut err = None;
  while size > 0 {
    match udp.set_recv_buffer_size(size) {
      Ok(_) => return Ok(()),
      Err(e) => err = Some(e),
    }
    size /= 2;
  }

  Err(err.unwrap_or_else(|| {
    std::io::Error::new(
      std::io::ErrorKind::Other,
      "fail to set receive buffer size for UDP socket",
    )
  }))
}
