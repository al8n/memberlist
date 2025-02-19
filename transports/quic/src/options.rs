use indexmap::IndexSet;

use super::*;

/// Used to configure a net transport.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize, A::Options: serde::Serialize, S::Options: serde::Serialize",
    deserialize = "I: serde::Deserialize<'de>, A: AddressResolver, A::Address: serde::Deserialize<'de>, A::ResolvedAddress: serde::Deserialize<'de>, A::Options: serde::Deserialize<'de>, S::Options: serde::Deserialize<'de>"
  ))
)]
pub struct QuicTransportOptions<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
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
      attrs(doc = "Get a list of addresses to bind to for QUIC communications."),
    ),
    setter(attrs(
      doc = "Set the list of addresses to bind to for QUIC communications. (Builder pattern)"
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

  /// The timeout used for I/O
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde::option"))]
  #[viewit(
    getter(const, attrs(doc = "Get timeout used for I/O."),),
    setter(attrs(doc = "Set timeout used for I/O. (Builder pattern)"),)
  )]
  timeout: Option<Duration>,

  /// The period of time to cleanup the connection pool.
  #[cfg_attr(
    feature = "serde",
    serde(
      with = "humantime_serde",
      default = "default_connection_pool_cleanup_period"
    )
  )]
  #[viewit(
    getter(const, attrs(doc = "Get the cleanup period for the connection pool."),),
    setter(attrs(doc = "Set the cleanup period for the connection pool"),)
  )]
  connection_pool_cleanup_period: Duration,

  /// The time to live for each connection in the connection pool. Default is `None`.
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the time to live for each connection in the connection pool."),
    ),
    setter(attrs(doc = "Set the time to live for each connection for the connection pool"),)
  )]
  connection_ttl: Option<Duration>,

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

  /// The metrics labels.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  #[viewit(
    getter(
      style = "ref",
      result(
        converter(fn = "Option::as_deref"),
        type = "Option<&memberlist_core::types::MetricLabels>"
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
  metric_labels: Option<Arc<memberlist_core::types::MetricLabels>>,
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer> Clone
  for QuicTransportOptions<I, A, S>
where
  I: Clone,
  A::Options: Clone,
  S::Options: Clone,
{
  fn clone(&self) -> Self {
    Self {
      id: self.id.clone(),
      bind_addresses: self.bind_addresses.clone(),
      connection_ttl: self.connection_ttl,
      resolver: self.resolver.clone(),
      stream_layer: self.stream_layer.clone(),
      timeout: self.timeout,
      connection_pool_cleanup_period: self.connection_pool_cleanup_period,
      cidrs_policy: self.cidrs_policy.clone(),
      #[cfg(feature = "metrics")]
      metric_labels: self.metric_labels.clone(),
    }
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  QuicTransportOptions<I, A, S>
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
  QuicTransportOptions<I, A, S>
where
  A::Options: Default,
{
  /// Creates a new net transport options by id, other configurations are left default.
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
  QuicTransportOptions<I, A, S>
where
  S::Options: Default,
{
  /// Creates a new net transport options by id, other configurations are left default.
  #[inline]
  pub fn with_resolver_options(id: I, resolver_options: A::Options) -> Self {
    Self::with_resolver_options_and_stream_layer_options(id, resolver_options, Default::default())
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  QuicTransportOptions<I, A, S>
{
  /// Creates a new net transport options by id, resolver options and stream layer options, other configurations are left default.
  pub fn with_resolver_options_and_stream_layer_options(
    id: I,
    resolver_options: A::Options,
    stream_layer_opts: S::Options,
  ) -> Self {
    Self {
      id,
      timeout: None,
      bind_addresses: IndexSet::new(),
      connection_ttl: None,
      resolver: resolver_options,
      stream_layer: stream_layer_opts,
      cidrs_policy: CIDRsPolicy::allow_all(),
      connection_pool_cleanup_period: default_connection_pool_cleanup_period(),
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

#[inline]
const fn default_connection_pool_cleanup_period() -> Duration {
  Duration::from_secs(60)
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  From<QuicTransportOptions<I, A, S>> for (A::Options, S::Options, Options<I, A>)
{
  fn from(opts: QuicTransportOptions<I, A, S>) -> Self {
    (
      opts.resolver,
      opts.stream_layer,
      Options {
        id: opts.id,
        bind_addresses: opts.bind_addresses,
        connection_ttl: opts.connection_ttl,
        timeout: opts.timeout,
        connection_pool_cleanup_period: opts.connection_pool_cleanup_period,
        cidrs_policy: opts.cidrs_policy,
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels,
      },
    )
  }
}

#[viewit::viewit(setters(skip), getters(skip))]
pub(crate) struct Options<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  id: I,
  bind_addresses: IndexSet<A::Address>,
  timeout: Option<Duration>,
  connection_pool_cleanup_period: Duration,
  connection_ttl: Option<Duration>,
  cidrs_policy: CIDRsPolicy,
  #[cfg(feature = "metrics")]
  metric_labels: Option<Arc<memberlist_core::types::MetricLabels>>,
}
