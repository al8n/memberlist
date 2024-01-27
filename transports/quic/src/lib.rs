mod stream_layer;
use std::{
  marker::PhantomData,
  net::{IpAddr, SocketAddr},
  sync::Arc,
};

use memberlist_core::{
  transport::{
    stream::{PacketSubscriber, StreamSubscriber},
    Transport, TransportError, Wire,
  },
  types::Message,
};
use memberlist_utils::{net::CIDRsPolicy, Label, SmallVec, TinyVec};
use nodecraft::{resolver::AddressResolver, CheapClone, Id};
pub use stream_layer::*;

/// Compress/decompress related.
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub mod compressor;

#[cfg(feature = "compression")]
use compressor::*;

const DEFAULT_PORT: u16 = 7946;

/// Errors that can occur when using [`NetTransport`].
#[derive(thiserror::Error)]
pub enum QuicTransportError<A: AddressResolver, S: StreamLayer, W: Wire> {
  /// Returns when there is no explicit advertise address and no private IP address found.
  #[error("no private IP address found, and explicit IP not provided")]
  NoPrivateIP,
  /// Returns when there is no interface addresses found.
  #[error("failed to get interface addresses {0}")]
  NoInterfaceAddresses(#[from] local_ip_address::Error),
  /// Returns when there is no bind address provided.
  #[error("at least one bind address is required")]
  EmptyBindAddresses,
  /// Returns when the ip is blocked.
  #[error("the ip {0} is blocked")]
  BlockedIp(IpAddr),
  /// Returns when the packet buffer size is too small.
  #[error("failed to resize packet buffer {0}")]
  ResizePacketBuffer(std::io::Error),
  /// Returns when the packet socket fails to bind.
  #[error("failed to start packet listener on {0}: {1}")]
  ListenPacket(SocketAddr, std::io::Error),
  /// Returns when the promised listener fails to bind.
  #[error("failed to start promised listener on {0}: {1}")]
  ListenPromised(SocketAddr, std::io::Error),
  /// Returns when we fail to resolve an address.
  #[error("failed to resolve address {addr}: {err}")]
  Resolve {
    /// The address we failed to resolve.
    addr: A::Address,
    /// The error that occurred.
    err: A::Error,
  },
  // /// Returns when the label error.
  // #[error("{0}")]
  // Label(#[from] label::LabelError),
  #[error(transparent)]
  Stream(S::Error),

  /// Returns when encode/decode error.
  #[error("wire error: {0}")]
  Wire(W::Error),
  /// Returns when the packet is too large.
  #[error("packet too large, the maximum packet can be sent is 65535, got {0}")]
  PacketTooLarge(usize),
  /// Returns when there is a custom error.
  #[error("custom error: {0}")]
  Custom(std::borrow::Cow<'static, str>),

  /// Returns when fail to compress/decompress message.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[error(transparent)]
  Compressor(#[from] compressor::CompressorError),

  /// Returns when the computation task panic
  #[error("computation task panic")]
  #[cfg(feature = "compression")]
  ComputationTaskFailed,
}

impl<A: AddressResolver, S: StreamLayer, W: Wire> core::fmt::Debug for QuicTransportError<A, S, W> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(&self, f)
  }
}

impl<A: AddressResolver, S: StreamLayer, W: Wire> TransportError for QuicTransportError<A, S, W> {
  fn is_remote_failure(&self) -> bool {
    if let Self::Stream(e) = self {
      e.is_remote_failure()
    } else {
      false
    }
  }

  fn custom(err: std::borrow::Cow<'static, str>) -> Self {
    Self::Custom(err)
  }
}

/// Used to configure a net transport.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize",
    deserialize = "I: for<'a> serde::Deserialize<'a>, A: AddressResolver, A::Address: for<'a> serde::Deserialize<'a>, A::ResolvedAddress: for<'a> serde::Deserialize<'a>"
  ))
)]
pub struct QuicTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  /// The local node's ID.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the id of the node."),),
    setter(attrs(doc = "Set the id of the node. (Builder pattern)"),)
  )]
  id: I,

  /// The local node's address.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the address of the node."),),
    setter(attrs(doc = "Set the address of the node. (Builder pattern)"),)
  )]
  address: A::Address,

  /// The address to advertise to other nodes. If not set,
  /// the transport will attempt to discover the local IP address
  /// to use.
  #[viewit(
    getter(const, attrs(doc = "Get the advertise address of the node."),),
    setter(attrs(doc = "Set the advertise address of the node. (Builder pattern)"),)
  )]
  advertise_address: Option<A::ResolvedAddress>,

  /// A list of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(doc = "Get a list of addresses to bind to for both TCP and UDP communications."),
    ),
    setter(attrs(
      doc = "Set the list of addresses to bind to for both TCP and UDP communications. (Builder pattern)"
    ),)
  )]
  bind_addresses: SmallVec<IpAddr>,

  /// The port for bind addresses of the node.
  ///
  /// Default is `7946`.
  #[viewit(
    getter(const, attrs(doc = "Get the port for bind address of the node."),),
    setter(attrs(doc = "Set the port for bind address of the node. (Builder pattern)"),)
  )]
  bind_port: Option<u16>,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the label of the node."),),
    setter(attrs(doc = "Set the label of the node. (Builder pattern)"),)
  )]
  label: Label,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get if the check that inbound packets and gossip streams need to be label prefixed."
      ),
    ),
    setter(attrs(
      doc = "Set if the check that inbound packets and gossip streams need to be label prefixed. (Builder pattern)"
    ),)
  )]
  skip_inbound_label_check: bool,

  /// Policy for Classless Inter-Domain Routing (CIDR).
  ///
  /// By default, allow any connection
  #[cfg_attr(feature = "serde", serde(default))]
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Get the policy for Classless Inter-Domain Routing (CIDR)."),
    ),
    setter(attrs(
      doc = "Set the policy for Classless Inter-Domain Routing (CIDR). (Builder pattern)"
    ),)
  )]
  cidrs_policy: CIDRsPolicy,

  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the compression algorithm used for outgoing.",
        cfg(feature = "compression"),
        cfg_attr(docsrs, doc(cfg(feature = "compression")))
      ),
    ),
    setter(attrs(
      doc = "Set the compression algorithm used for outgoing. (Builder pattern)",
      cfg(feature = "compression"),
      cfg_attr(docsrs, doc(cfg(feature = "compression")))
    ),)
  )]
  compressor: Option<Compressor>,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  #[cfg(feature = "compression")]
  #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the size of a message that should be offload to [`rayon`] thread pool for encryption or compression.",
        cfg(feature = "compression"),
        cfg_attr(docsrs, doc(cfg(feature = "compression")))
      ),
    ),
    setter(attrs(
      doc = "Set the size of a message that should be offload to [`rayon`] thread pool for encryption or compression. (Builder pattern)",
      cfg(feature = "compression"),
      cfg_attr(docsrs, doc(cfg(feature = "compression")))
    ),)
  )]
  offload_size: usize,

  /// The metrics labels.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  #[viewit(
    getter(
      style = "ref",
      result(
        converter(fn = "Option::as_deref"),
        type = "Option<&memberlist_utils::MetricLabels>"
      ),
      attrs(
        doc = "Get the metrics labels.",
        cfg(feature = "metrics"),
        cfg_attr(docsrs, doc(cfg(feature = "metrics")))
      ),
    ),
    setter(attrs(
      doc = "Set the metrics labels. (Builder pattern)",
      cfg(feature = "metrics"),
      cfg_attr(docsrs, doc(cfg(feature = "metrics")))
    ))
  )]
  metric_labels: Option<Arc<memberlist_utils::MetricLabels>>,
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>> QuicTransportOptions<I, A> {
  /// Creates a new net transport options by id and address, other configurations are left default.
  pub fn new(id: I, address: A::Address) -> Self {
    Self {
      id,
      address,
      advertise_address: None,
      bind_addresses: SmallVec::new(),
      bind_port: Some(DEFAULT_PORT),
      label: Label::empty(),
      skip_inbound_label_check: false,
      cidrs_policy: CIDRsPolicy::allow_all(),
      #[cfg(feature = "compression")]
      compressor: None,
      #[cfg(feature = "compression")]
      offload_size: 1024,
      #[cfg(feature = "metrics")]
      metric_labels: None,
    }
  }
}

/// A [`Transport`] implementation based on QUIC
pub struct QuicTransport<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer, W> {
  opts: Arc<QuicTransportOptions<I, A>>,
  advertise_addr: A::ResolvedAddress,
  packet_rx: PacketSubscriber<I, A::ResolvedAddress>,
  stream_rx: StreamSubscriber<A::ResolvedAddress, S::Stream>,
  stream_layer: Arc<S>,

  // wg: AsyncWaitGroup,
  resolver: Arc<A>,
  // shutdown: Arc<AtomicBool>,
  shutdown_tx: async_channel::Sender<()>,

  max_payload_size: usize,
  packet_overhead: usize,
  max_num_packets: usize,
  packet_header_overhead: usize,
  fix_packet_overhead: usize,
  _marker: PhantomData<W>,
}

// impl<I, A, S, W> QuicTransport<I, A, S, W>
// where
//   I: Id,
//   A: AddressResolver<ResolvedAddress = SocketAddr>,
//   S: StreamLayer,
//   W: Wire<Id = I, Address = A::ResolvedAddress>,
// {
//   fn fix_packet_overhead(&self) -> usize {
//     let mut overhead = self.opts.label.encoded_overhead();

//     #[cfg(feature = "compression")]
//     if self.opts.compressor.is_some() {
//       overhead += 1 + core::mem::size_of::<u32>();
//     }

//     overhead
//   }
// }

impl<I, A, S, W> Transport for QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  type Error = QuicTransportError<A, S, W>;

  type Id = I;

  type Resolver = A;

  type Stream = S::Stream;

  type Wire = W;

  type Runtime = A::Runtime;

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

  #[inline(always)]
  fn local_id(&self) -> &Self::Id {
    &self.opts.id
  }

  #[inline(always)]
  fn local_address(&self) -> &<Self::Resolver as AddressResolver>::Address {
    &self.opts.address
  }

  #[inline(always)]
  fn advertise_address(&self) -> &<Self::Resolver as AddressResolver>::ResolvedAddress {
    &self.advertise_addr
  }

  #[inline(always)]
  fn max_payload_size(&self) -> usize {
    self.max_payload_size
  }

  #[inline(always)]
  fn packet_overhead(&self) -> usize {
    self.packet_overhead
  }

  #[inline(always)]
  fn packets_header_overhead(&self) -> usize {
    self.fix_packet_overhead + self.packet_header_overhead
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
    todo!()
  }

  async fn send_message(
    &self,
    conn: &mut Self::Stream,
    msg: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<usize, Self::Error> {
    todo!()
  }

  async fn send_packet(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packet: Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(usize, std::time::Instant), Self::Error> {
    todo!()
  }

  async fn send_packets(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    packets: TinyVec<Message<Self::Id, <Self::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<(usize, std::time::Instant), Self::Error> {
    todo!()
  }

  async fn dial_timeout(
    &self,
    addr: &<Self::Resolver as AddressResolver>::ResolvedAddress,
    timeout: std::time::Duration,
  ) -> Result<Self::Stream, Self::Error> {
    todo!()
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
    todo!()
  }

  fn block_shutdown(&self) -> Result<(), Self::Error> {
    todo!()
  }
}
