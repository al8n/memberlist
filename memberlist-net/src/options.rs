use std::net::SocketAddr;

use indexmap::IndexSet;
use memberlist_core::proto::CIDRsPolicy;
use nodecraft::resolver::AddressResolver;

use crate::StreamLayer;

/// Used to configure a net transport.
#[viewit::viewit(
  vis_all = "pub(crate)",
  getters(vis_all = "pub"),
  setters(vis_all = "pub", prefix = "with")
)]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize, A::Options: serde::Serialize, S::Options: serde::Serialize",
    deserialize = "I: serde::Deserialize<'de>, A: AddressResolver, A::Address: serde::Deserialize<'de>, A::ResolvedAddress: serde::Deserialize<'de>, A::Options: serde::Deserialize<'de>, S::Options: serde::Deserialize<'de>"
  ))
)]
pub struct NetTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
{
  /// The local node's ID.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the id of the node."),),
    setter(attrs(doc = "Set the id of the node. (Builder pattern)"),)
  )]
  id: I,

  /// A set of addresses to bind to for both TCP and UDP
  /// communications.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(doc = "Get a set of addresses to bind to for both TCP and UDP communications."),
    ),
    setter(attrs(
      doc = "Set the set of addresses to bind to for both TCP and UDP communications. (Builder pattern)"
    ),)
  )]
  bind_addresses: IndexSet<A::Address>,

  /// Resolver options, which used to construct the address resolver for this transport.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the address resolver options."),),
    setter(attrs(doc = "Set the address resolver options. (Builder pattern)"),)
  )]
  resolver: A::Options,

  /// Stream layer options, which used to construct the stream layer for this transport.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the stream layer options."),),
    setter(attrs(doc = "Set the stream layer options. (Builder pattern)"),)
  )]
  stream_layer: S::Options,

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

  /// Set the maximum packet size can be sent by UDP
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the maximum payload size can be sent by UDP. Default is `1472` bytes."),
    ),
    setter(attrs(
      doc = "Set the maximum payload size can be sent by UDP. Default is `1472` bytes. (Builder pattern)"
    ),)
  )]
  max_packet_size: usize,

  /// Set the receive buffer size of UDP
  #[viewit(
    getter(const, attrs(doc = "Get the UDP receive window. Default is `2MB`."),),
    setter(attrs(doc = "Set the UDP receive window. Default is `2MB`. (Builder pattern)"),)
  )]
  recv_buffer_size: usize,

  /// The metrics labels.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  #[viewit(
    getter(
      style = "ref",
      result(
        converter(fn = "Option::as_deref"),
        type = "Option<&memberlist_core::proto::MetricLabels>"
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
  #[cfg_attr(
    feature = "serde",
    serde(default, skip_serializing_if = "Option::is_none")
  )]
  metric_labels: Option<std::sync::Arc<memberlist_core::proto::MetricLabels>>,
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer> Clone
  for NetTransportOptions<I, A, S>
where
  I: Clone,
  A::Options: Clone,
  S::Options: Clone,
{
  fn clone(&self) -> Self {
    Self {
      id: self.id.clone(),
      bind_addresses: self.bind_addresses.clone(),
      stream_layer: self.stream_layer.clone(),
      resolver: self.resolver.clone(),
      cidrs_policy: self.cidrs_policy.clone(),
      max_packet_size: self.max_packet_size,
      recv_buffer_size: self.recv_buffer_size,
      #[cfg(feature = "metrics")]
      metric_labels: self.metric_labels.clone(),
    }
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  NetTransportOptions<I, A, S>
where
  A::Options: Default,
  S::Options: Default,
{
  /// Creates a new net transport options by id, other configurations are left default.
  #[inline]
  pub fn new(id: I) -> Self {
    Self::with_resolver_options_and_stream_layer_options(id, Default::default(), Default::default())
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  NetTransportOptions<I, A, S>
where
  S::Options: Default,
{
  /// Creates a new net transport options by id and resolver options, other configurations are left default.
  #[inline]
  pub fn with_resolver_options(id: I, resolver_options: A::Options) -> Self {
    Self::with_resolver_options_and_stream_layer_options(id, resolver_options, Default::default())
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  NetTransportOptions<I, A, S>
where
  A::Options: Default,
{
  /// Creates a new net transport options by id and stream layer options, other configurations are left default.
  #[inline]
  pub fn with_stream_layer_options(id: I, stream_layer_options: S::Options) -> Self {
    Self::with_resolver_options_and_stream_layer_options(
      id,
      Default::default(),
      stream_layer_options,
    )
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  NetTransportOptions<I, A, S>
{
  /// Creates a new net transport options by id, resolver options and stream layer options, other configurations are left default.
  pub fn with_resolver_options_and_stream_layer_options(
    id: I,
    resolver_options: A::Options,
    stream_layer_opts: S::Options,
  ) -> Self {
    Self {
      id,
      bind_addresses: IndexSet::new(),
      resolver: resolver_options,
      stream_layer: stream_layer_opts,
      cidrs_policy: CIDRsPolicy::allow_all(),
      max_packet_size: 1472,
      recv_buffer_size: super::DEFAULT_UDP_RECV_BUF_SIZE,
      #[cfg(feature = "metrics")]
      metric_labels: None,
    }
  }

  /// Add bind address
  pub fn add_bind_address(&mut self, addr: A::Address) -> &mut Self {
    self.bind_addresses.insert(addr);
    self
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  From<NetTransportOptions<I, A, S>> for (A::Options, S::Options, Options<I, A>)
{
  fn from(opts: NetTransportOptions<I, A, S>) -> (A::Options, S::Options, Options<I, A>) {
    (
      opts.resolver,
      opts.stream_layer,
      Options {
        id: opts.id,
        bind_addresses: opts.bind_addresses,
        cidrs_policy: opts.cidrs_policy,
        max_packet_size: opts.max_packet_size,
        recv_buffer_size: opts.recv_buffer_size,
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels,
      },
    )
  }
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct Options<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  id: I,
  bind_addresses: IndexSet<A::Address>,
  cidrs_policy: CIDRsPolicy,
  max_packet_size: usize,
  recv_buffer_size: usize,
  #[cfg(feature = "metrics")]
  metric_labels: Option<std::sync::Arc<memberlist_core::proto::MetricLabels>>,
}
