use std::net::SocketAddr;

use indexmap::IndexSet;
use memberlist_core::types::{CIDRsPolicy, Label};
use nodecraft::resolver::AddressResolver;

use crate::{Checksumer, StreamLayer};

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
    serialize = "I: serde::Serialize, A: AddressResolver, A::Address: serde::Serialize, A::ResolvedAddress: serde::Serialize, S::Options: serde::Serialize",
    deserialize = "I: serde::Deserialize<'de>, A: AddressResolver, A::Address: serde::Deserialize<'de>, A::ResolvedAddress: serde::Deserialize<'de>, S::Options: serde::Deserialize<'de>"
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

  /// Set the maximum payload size can be sent by UDP
  #[viewit(
    getter(const, attrs(doc = "Get the maximum payload size can be sent by UDP."),),
    setter(attrs(doc = "Set the maximum payload size can be sent by UDP. (Builder pattern)"),)
  )]
  max_payload_size: usize,

  /// The checksumer to use for checksumming packets.
  #[cfg_attr(feature = "serde", serde(default))]
  #[viewit(
    getter(
      const,
      attrs(doc = "Get the checksumer used to calculate checksum for UDP."),
    ),
    setter(attrs(
      doc = "Set the checksumer used to calculate checksum for UDP. (Builder pattern)"
    ),)
  )]
  checksumer: Checksumer,

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
  compressor: Option<super::compressor::Compressor>,

  /// Controls whether to enforce encryption for outgoing
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  #[cfg_attr(feature = "serde", serde(default))]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get whether to enforce encryption for outgoing gossip. It is used for upshifting from unencrypted to encrypted gossip on a running cluster.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      ),
    ),
    setter(attrs(
      doc = "Set whether to enforce encryption for outgoing gossip. It is used for upshifting from unencrypted to encrypted gossip on a running cluster. (Builder pattern)",
      cfg(feature = "encryption"),
      cfg_attr(docsrs, doc(cfg(feature = "encryption")))
    ),)
  )]
  gossip_verify_outgoing: bool,

  /// Controls whether to enforce encryption for incoming
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  #[cfg_attr(feature = "serde", serde(default))]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get whether to enforce encryption for incoming gossip. It is used for upshifting from unencrypted to encrypted gossip on a running cluster.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      ),
    ),
    setter(attrs(
      doc = "Set whether to enforce encryption for incoming gossip. It is used for upshifting from unencrypted to encrypted gossip on a running cluster. (Builder pattern)",
      cfg(feature = "encryption"),
      cfg_attr(docsrs, doc(cfg(feature = "encryption")))
    ),)
  )]
  gossip_verify_incoming: bool,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  #[cfg(any(feature = "compression", feature = "encryption"))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "compression", feature = "encryption"))))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Get the size of a message that should be offload to [`rayon`] thread pool for encryption or compression.",
        cfg(any(feature = "compression", feature = "encryption")),
        cfg_attr(docsrs, doc(cfg(any(feature = "compression", feature = "encryption"))))
      ),
    ),
    setter(attrs(
      doc = "Set the size of a message that should be offload to [`rayon`] thread pool for encryption or compression. (Builder pattern)",
      cfg(any(feature = "compression", feature = "encryption")),
      cfg_attr(docsrs, doc(cfg(any(feature = "compression", feature = "encryption"))))
    ),)
  )]
  offload_size: usize,

  /// Used to initialize the primary encryption key in a keyring.
  ///
  /// **Note: This field will not be used when network layer is secure**
  ///
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[viewit(
    getter(
      const,
      style = "ref",
      result(
        converter(fn = "Option::as_ref"),
        type = "Option<&super::security::SecretKey>"
      ),
      attrs(
        doc = "Get the primary encryption key in a keyring.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      ),
    ),
    setter(attrs(
      doc = "Set the primary encryption key in a keyring. (Builder pattern)",
      cfg(feature = "encryption"),
      cfg_attr(docsrs, doc(cfg(feature = "encryption")))
    ),)
  )]
  primary_key: Option<super::security::SecretKey>,

  /// Holds all of the encryption keys used internally.
  ///
  /// **Note: This field will not be used if the network layer is secure.**
  #[viewit(
    getter(
      style = "ref",
      result(
        converter(fn = "Option::as_ref"),
        type = "Option<&super::security::SecretKeys>"
      ),
      attrs(
        doc = "Get all of the encryption keys used internally.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      ),
    ),
    setter(attrs(
      doc = "Set all of the encryption keys used internally. (Builder pattern)",
      cfg(feature = "encryption"),
      cfg_attr(docsrs, doc(cfg(feature = "encryption")))
    ))
  )]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  secret_keys: Option<super::security::SecretKeys>,

  /// The configured encryption type that we
  /// will _speak_.
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[viewit(
    getter(
      style = "ref",
      result(
        converter(fn = "Option::as_ref"),
        type = "Option<&super::security::EncryptionAlgo>"
      ),
      attrs(
        doc = "Get the encryption algorithm used to encrypt the outgoing gossip.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      ),
    ),
    setter(attrs(
      doc = "Set the encryption algorithm used to encrypt the outgoing gossip. (Builder pattern)",
      cfg(feature = "encryption"),
      cfg_attr(docsrs, doc(cfg(feature = "encryption")))
    ))
  )]
  encryption_algo: Option<super::security::EncryptionAlgo>,

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
  metric_labels: Option<std::sync::Arc<memberlist_core::types::MetricLabels>>,
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
      label: self.label.clone(),
      stream_layer_options: self.stream_layer_options.clone(),
      skip_inbound_label_check: self.skip_inbound_label_check,
      cidrs_policy: self.cidrs_policy.clone(),
      max_payload_size: self.max_payload_size,
      checksumer: self.checksumer,
      #[cfg(feature = "compression")]
      compressor: self.compressor.clone(),
      #[cfg(any(feature = "compression", feature = "encryption"))]
      offload_size: self.offload_size,
      #[cfg(feature = "encryption")]
      gossip_verify_outgoing: self.gossip_verify_outgoing,
      #[cfg(feature = "encryption")]
      gossip_verify_incoming: self.gossip_verify_incoming,
      #[cfg(feature = "encryption")]
      primary_key: self.primary_key.clone(),
      #[cfg(feature = "encryption")]
      secret_keys: self.secret_keys.clone(),
      #[cfg(feature = "encryption")]
      encryption_algo: self.encryption_algo.clone(),
      #[cfg(feature = "metrics")]
      metric_labels: self.metric_labels.clone(),
    }
  }
}

impl<I, A: AddressResolver<ResolvedAddress = SocketAddr>, S: StreamLayer>
  NetTransportOptions<I, A, S>
{
  /// Creates a new net transport options by id and address, other configurations are left default.
  pub fn new(id: I, stream_layer_opts: S::Options) -> Self {
    Self {
      id,
      // advertise_address: None,
      bind_addresses: IndexSet::new(),
      label: Label::empty(),
      stream_layer_options: stream_layer_opts,
      skip_inbound_label_check: false,
      cidrs_policy: CIDRsPolicy::allow_all(),
      max_payload_size: 1400,
      checksumer: Checksumer::Crc32,
      #[cfg(feature = "encryption")]
      gossip_verify_outgoing: false,
      #[cfg(feature = "encryption")]
      gossip_verify_incoming: false,
      #[cfg(feature = "compression")]
      compressor: None,
      #[cfg(any(feature = "compression", feature = "encryption"))]
      offload_size: 1024,
      #[cfg(feature = "encryption")]
      primary_key: None,
      #[cfg(feature = "encryption")]
      secret_keys: None,
      #[cfg(feature = "encryption")]
      encryption_algo: None,
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
  From<NetTransportOptions<I, A, S>> for (S::Options, Options<I, A>)
{
  fn from(opts: NetTransportOptions<I, A, S>) -> (S::Options, Options<I, A>) {
    (
      opts.stream_layer_options,
      Options {
        id: opts.id,
        bind_addresses: opts.bind_addresses,
        label: opts.label,
        skip_inbound_label_check: opts.skip_inbound_label_check,
        cidrs_policy: opts.cidrs_policy,
        max_payload_size: opts.max_payload_size,
        checksumer: opts.checksumer,
        #[cfg(feature = "compression")]
        compressor: opts.compressor,
        #[cfg(feature = "encryption")]
        gossip_verify_outgoing: opts.gossip_verify_outgoing,
        #[cfg(feature = "encryption")]
        gossip_verify_incoming: opts.gossip_verify_incoming,
        #[cfg(any(feature = "compression", feature = "encryption"))]
        offload_size: opts.offload_size,
        #[cfg(feature = "encryption")]
        primary_key: opts.primary_key,
        #[cfg(feature = "encryption")]
        secret_keys: opts.secret_keys,
        #[cfg(feature = "encryption")]
        encryption_algo: opts.encryption_algo,
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
  cidrs_policy: CIDRsPolicy,
  max_payload_size: usize,
  checksumer: Checksumer,
  #[cfg(feature = "compression")]
  compressor: Option<super::compressor::Compressor>,
  #[cfg(feature = "encryption")]
  gossip_verify_outgoing: bool,
  #[cfg(feature = "encryption")]
  gossip_verify_incoming: bool,
  #[cfg(any(feature = "compression", feature = "encryption"))]
  offload_size: usize,
  #[cfg(feature = "encryption")]
  primary_key: Option<super::security::SecretKey>,
  #[cfg(feature = "encryption")]
  secret_keys: Option<super::security::SecretKeys>,
  #[cfg(feature = "encryption")]
  encryption_algo: Option<super::security::EncryptionAlgo>,
  #[cfg(feature = "metrics")]
  metric_labels: Option<std::sync::Arc<memberlist_core::types::MetricLabels>>,
}
