use indexmap::IndexSet;

use super::*;

/// Used to configure a net transport.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize, S::Options: serde::Serialize",
    deserialize = "I: for<'a> serde::Deserialize<'a>, A: AddressResolver, A::Address: for<'a> serde::Deserialize<'a>, A::ResolvedAddress: for<'a> serde::Deserialize<'a>, S::Options: serde::Deserialize<'de>"
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

  /// Stream layer options, which used to construct the stream layer for this transport.
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Get the stream layer options."),),
    setter(attrs(doc = "Set the stream layer options. (Builder pattern)"),)
  )]
  stream_layer_options: S::Options,

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

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  QuicTransportOptions<I, A, S>
{
  /// Creates a new net transport options by id and address, other configurations are left default.
  pub fn new(id: I, stream_layer_opts: S::Options) -> Self {
    Self {
      id,
      timeout: None,
      bind_addresses: IndexSet::new(),
      label: Label::empty(),
      stream_layer_options: stream_layer_opts,
      skip_inbound_label_check: false,
      cidrs_policy: CIDRsPolicy::allow_all(),
      connection_pool_cleanup_period: default_connection_pool_cleanup_period(),
      #[cfg(feature = "compression")]
      compressor: None,
      #[cfg(feature = "compression")]
      offload_size: 1024,
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
  From<QuicTransportOptions<I, A, S>> for (S::Options, Options<I, A>)
{
  fn from(opts: QuicTransportOptions<I, A, S>) -> Self {
    (
      opts.stream_layer_options,
      Options {
        id: opts.id,
        bind_addresses: opts.bind_addresses,
        label: opts.label,
        skip_inbound_label_check: opts.skip_inbound_label_check,
        timeout: opts.timeout,
        connection_pool_cleanup_period: opts.connection_pool_cleanup_period,
        cidrs_policy: opts.cidrs_policy,
        #[cfg(feature = "compression")]
        compressor: opts.compressor,
        #[cfg(feature = "compression")]
        offload_size: opts.offload_size,
        #[cfg(feature = "metrics")]
        metric_labels: opts.metric_labels,
      },
    )
  }
}

#[viewit::viewit]
pub(crate) struct Options<I, A: AddressResolver<ResolvedAddress = SocketAddr>> {
  id: I,
  bind_addresses: IndexSet<A::Address>,
  label: Label,
  skip_inbound_label_check: bool,
  timeout: Option<Duration>,
  connection_pool_cleanup_period: Duration,
  cidrs_policy: CIDRsPolicy,
  #[cfg(feature = "compression")]
  compressor: Option<Compressor>,
  #[cfg(feature = "compression")]
  offload_size: usize,
  #[cfg(feature = "metrics")]
  metric_labels: Option<Arc<memberlist_core::types::MetricLabels>>,
}
