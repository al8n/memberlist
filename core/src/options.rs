use std::time::Duration;

use memberlist_types::Label;

use super::types::{DelegateVersion, ProtocolVersion};

use super::types::ChecksumAlgorithm;

#[cfg(feature = "compression")]
use super::types::CompressAlgorithm;

#[cfg(feature = "encryption")]
use super::types::EncryptionAlgorithm;

#[cfg(feature = "metrics")]
pub use super::types::MetricLabels;

/// Options used to configure the memberlist.
#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Options {
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

  /// The timeout for establishing a stream connection with
  /// a remote node for a full state sync, and for stream read and write
  /// operations.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Returns the timeout for establishing a stream connection with a remote node for a full state sync, and for stream read and write operations."
      )
    ),
    setter(
      const,
      attrs(
        doc = "Sets the timeout for establishing a stream connection with a remote node for a full state sync, and for stream read and write operations (Builder pattern)."
      )
    )
  )]
  timeout: Duration,

  /// The number of nodes that will be asked to perform
  /// an indirect probe of a node in the case a direct probe fails. [`Memberlist`](crate::Memberlist)
  /// waits for an ack from any single indirect node, so increasing this
  /// number will increase the likelihood that an indirect probe will succeed
  /// at the expense of bandwidth.
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Returns the number of nodes that will be asked to perform an indirect probe of a node in the case a direct probe fails."
      )
    ),
    setter(
      const,
      attrs(
        doc = "Sets the number of nodes that will be asked to perform an indirect probe of a node in the case a direct probe fails (Builder pattern)."
      )
    )
  )]
  indirect_checks: usize,

  /// The multiplier for the number of retransmissions
  /// that are attempted for messages broadcasted over gossip. The actual
  /// count of retransmissions is calculated using the formula:
  ///
  ///   `retransmits = retransmit_mult * log(N+1)`
  ///
  /// This allows the retransmits to scale properly with cluster size. The
  /// higher the multiplier, the more likely a failed broadcast is to converge
  /// at the expense of increased bandwidth.
  #[viewit(
    getter(const, attrs(doc = "Returns the retransmit mult")),
    setter(const, attrs(doc = "Sets the retransmit mult (Builder pattern)."))
  )]
  retransmit_mult: usize,

  /// The multiplier for determining the time an
  /// inaccessible node is considered suspect before declaring it dead.
  /// The actual timeout is calculated using the formula:
  ///
  ///   `suspicion_timeout = suspicion_mult * log(N+1) * probe_interval`
  ///
  /// This allows the timeout to scale properly with expected propagation
  /// delay with a larger cluster size. The higher the multiplier, the longer
  /// an inaccessible node is considered part of the cluster before declaring
  /// it dead, giving that suspect node more time to refute if it is indeed
  /// still alive.
  #[viewit(
    getter(const, attrs(doc = "Returns the suspicion mult")),
    setter(const, attrs(doc = "Sets the suspicion mult (Builder pattern)."))
  )]
  suspicion_mult: usize,

  /// The multiplier applied to the
  /// `suspicion_timeout` used as an upper bound on detection time. This max
  /// timeout is calculated using the formula:
  ///
  /// `suspicion_max_timeout = suspicion_max_timeout_mult * suspicion_timeout`
  ///
  /// If everything is working properly, confirmations from other nodes will
  /// accelerate suspicion timers in a manner which will cause the timeout
  /// to reach the base SuspicionTimeout before that elapses, so this value
  /// will typically only come into play if a node is experiencing issues
  /// communicating with other nodes. It should be set to a something fairly
  /// large so that a node having problems will have a lot of chances to
  /// recover before falsely declaring other nodes as failed, but short
  /// enough for a legitimately isolated node to still make progress marking
  /// nodes failed in a reasonable amount of time.
  #[viewit(
    getter(const, attrs(doc = "Returns the suspicion max timeout mult")),
    setter(
      const,
      attrs(doc = "Sets the suspicion max timeout mult (Builder pattern).")
    )
  )]
  suspicion_max_timeout_mult: usize,

  /// The interval between complete state syncs.
  /// Complete state syncs are done with a single node over TCP and are
  /// quite expensive relative to standard gossiped messages. Setting this
  /// to zero will disable state push/pull syncs completely.
  ///
  /// Setting this interval lower (more frequent) will increase convergence
  /// speeds across larger clusters at the expense of increased bandwidth
  /// usage.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the push pull interval")),
    setter(const, attrs(doc = "Sets the push pull interval (Builder pattern)."))
  )]
  push_pull_interval: Duration,

  /// The interval between random node probes. Setting
  /// this lower (more frequent) will cause the memberlist cluster to detect
  /// failed nodes more quickly at the expense of increased bandwidth usage
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the probe interval")),
    setter(const, attrs(doc = "Sets the probe interval (Builder pattern)."))
  )]
  probe_interval: Duration,
  /// The timeout to wait for an ack from a probed node
  /// before assuming it is unhealthy. This should be set to 99-percentile
  /// of RTT (round-trip time) on your network.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the probe timeout")),
    setter(const, attrs(doc = "Sets the probe timeout (Builder pattern)."))
  )]
  probe_timeout: Duration,

  /// Set this field will turn off the fallback promised pings that are attempted
  /// if the direct unreliable ping fails. These get pipelined along with the
  /// indirect unreliable pings.
  #[viewit(
    getter(const, attrs(doc = "Returns whether disable promised pings or not")),
    setter(
      const,
      attrs(doc = "Sets whether disable promised pings or not (Builder pattern).")
    )
  )]
  disable_reliable_pings: bool,

  /// Increase the probe interval if the node
  /// becomes aware that it might be degraded and not meeting the soft real
  /// time requirements to reliably probe other nodes.
  #[viewit(
    getter(const, attrs(doc = "Returns the awareness max multiplier")),
    setter(
      const,
      attrs(doc = "Sets the awareness max multiplier (Builder pattern).")
    )
  )]
  awareness_max_multiplier: usize,

  /// The interval between sending messages that need
  /// to be gossiped that haven't been able to piggyback on probing messages.
  /// If this is set to zero, non-piggyback gossip is disabled. By lowering
  /// this value (more frequent) gossip messages are propagated across
  /// the cluster more quickly at the expense of increased bandwidth.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the gossip interval")),
    setter(const, attrs(doc = "Sets the gossip interval (Builder pattern)."))
  )]
  gossip_interval: Duration,

  /// The number of random nodes to send gossip messages to
  /// per `gossip_interval`. Increasing this number causes the gossip messages
  /// to propagate across the cluster more quickly at the expense of
  /// increased bandwidth.
  #[viewit(
    getter(const, attrs(doc = "Returns the gossip nodes")),
    setter(const, attrs(doc = "Sets the gossip nodes (Builder pattern)."))
  )]
  gossip_nodes: usize,
  /// The interval after which a node has died that
  /// we will still try to gossip to it. This gives it a chance to refute.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the gossip to the dead timeout")),
    setter(
      const,
      attrs(doc = "Sets the gossip to the dead timeout (Builder pattern).")
    )
  )]
  gossip_to_the_dead_time: Duration,

  /// Used to guarantee protocol-compatibility
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the protocol version this node is speaking")
    ),
    setter(
      const,
      attrs(doc = "Sets the protocol version this node is speaking (Builder pattern).")
    )
  )]
  protocol_version: ProtocolVersion,

  // #[viewit(getter(style = "ref", result(converter(fn = "Option::as_ref"), type = "Option<&Keyring>")))]
  // secret_keyring: Option<Keyring>,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the delegate version this node is speaking")
    ),
    setter(
      const,
      attrs(doc = "Sets the delegate version this node is speaking (Builder pattern).")
    )
  )]
  delegate_version: DelegateVersion,

  /// Size of Memberlist's internal channel which handles UDP messages. The
  /// size of this determines the size of the queue which Memberlist will keep
  /// while UDP messages are handled.
  #[viewit(
    getter(const, attrs(doc = "Returns the handoff queue depth")),
    setter(const, attrs(doc = "Sets the handoff queue depth (Builder pattern)."))
  )]
  handoff_queue_depth: usize,

  /// Controls the time before a dead node's name can be
  /// reclaimed by one with a different address or port. By default, this is 0,
  /// meaning nodes cannot be reclaimed this way.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the dead node reclaim time")),
    setter(
      const,
      attrs(doc = "Sets the dead node reclaim time (Builder pattern).")
    )
  )]
  dead_node_reclaim_time: Duration,

  /// The interval at which we check the message
  /// queue to apply the warning and max depth.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  #[viewit(
    getter(const, attrs(doc = "Returns the queue check interval")),
    setter(const, attrs(doc = "Sets the queue check interval (Builder pattern)."))
  )]
  queue_check_interval: Duration,

  /// Indicates that should the messages sent as packets through transport will be appended a checksum.
  ///
  /// Default is `None`.
  ///
  /// ## Note
  /// If the [`Transport::packet_reliable`](crate::transport::Transport::packet_reliable) is return `true`,
  /// then the checksum will not be appended to the packets, even if this field is set to `Some`.
  #[cfg_attr(
    feature = "serde",
    serde(skip_serializing_if = "Option::is_none", default)
  )]
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the checksum algorithm for the packets sent through transport.",)
    ),
    setter(
      const,
      attrs(doc = "Sets the checksum algorithm for the packets sent through transport.",)
    )
  )]
  checksum_algo: Option<ChecksumAlgorithm>,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for checksuming, encryption or compression.
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

  /// Indicates that should the messages sent as packet or stream through transport will be encrypted or not.
  ///
  /// Default is `None`.
  ///
  /// ## Note
  /// - If the [`Transport::packet_secure`](crate::transport::Transport::packet_secure) returns `true`,
  ///   then the encryption will not be applied to the messages when sending as packet, even if this field is set to `Some`.
  /// - If the [`Transport::stream_secure`](crate::transport::Transport::stream_secure) returns `true`,
  ///   then the encryption will not be applied to the messages when sending as stream, even if this field is set to `Some`.
  #[cfg(feature = "encryption")]
  #[cfg_attr(
    feature = "serde",
    serde(skip_serializing_if = "Option::is_none", default)
  )]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Returns the encryption algorithm for the messages sent through transport.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      )
    ),
    setter(
      const,
      attrs(
        doc = "Sets the encryption algorithm for the messages sent through transport.",
        cfg(feature = "encryption"),
        cfg_attr(docsrs, doc(cfg(feature = "encryption")))
      )
    )
  )]
  encryption_algo: Option<EncryptionAlgorithm>,

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

  /// Indicates that should the messages sent through transport will be compressed or not.
  ///
  /// Default is `None`.
  #[cfg(feature = "compression")]
  #[cfg_attr(
    feature = "serde",
    serde(skip_serializing_if = "Option::is_none", default)
  )]
  #[viewit(
    getter(
      const,
      attrs(
        doc = "Returns the compress algorithm for the messages sent through transport.",
        cfg(feature = "compression"),
        cfg_attr(docsrs, doc(cfg(feature = "compression")))
      )
    ),
    setter(
      const,
      attrs(
        doc = "Sets the compress algorithm for the messages sent through transport.",
        cfg(feature = "compression"),
        cfg_attr(docsrs, doc(cfg(feature = "compression")))
      )
    )
  )]
  compress_algo: Option<CompressAlgorithm>,

  /// The metric labels for the memberlist.
  #[viewit(
    getter(
      style = "ref",
      const,
      attrs(
        doc = "Get the metric labels for the memberlist.",
        cfg(feature = "metrics"),
        cfg_attr(docsrs, doc(cfg(feature = "metrics")))
      )
    ),
    setter(attrs(
      doc = "Sets the metric labels for the memberlist.",
      cfg(feature = "metrics"),
      cfg_attr(docsrs, doc(cfg(feature = "metrics")))
    ))
  )]
  #[cfg(feature = "metrics")]
  metric_labels: std::sync::Arc<MetricLabels>,
}

impl Default for Options {
  #[inline]
  fn default() -> Self {
    Self::lan()
  }
}

impl Options {
  /// Returns a sane set of configurations for Memberlist.
  /// Sets very conservative
  /// values that are sane for most LAN environments. The default configuration
  /// errs on the side of caution, choosing values that are optimized
  /// for higher convergence at the cost of higher bandwidth usage. Regardless,
  /// these values are a good starting point when getting started with memberlist.
  #[inline]
  pub fn lan() -> Self {
    Self {
      label: Label::empty(),
      timeout: Duration::from_secs(10), // Timeout after 10 seconds
      indirect_checks: 3,               // Use 3 nodes for the indirect ping
      retransmit_mult: 4,               // Retransmit a message 4 * log(N+1) nodes
      suspicion_mult: 4,                // Suspect a node for 4 * log(N+1) * Interval
      suspicion_max_timeout_mult: 6,    // For 10k nodes this will give a max timeout of 120 seconds
      push_pull_interval: Duration::from_secs(30), // Low frequency
      probe_interval: Duration::from_millis(500), // Failure check every second
      probe_timeout: Duration::from_secs(1), // Reasonable RTT time for LAN
      disable_reliable_pings: false,    // TCP pings are safe, even with mixed versions
      awareness_max_multiplier: 8,      // Probe interval backs off to 8 seconds
      gossip_interval: Duration::from_millis(200), // Gossip every 200ms
      gossip_nodes: 3,                  // Gossip to 3 nodes
      gossip_to_the_dead_time: Duration::from_secs(30), // same as push/pull
      delegate_version: DelegateVersion::V1,
      protocol_version: ProtocolVersion::V1,
      handoff_queue_depth: 1024,
      dead_node_reclaim_time: Duration::ZERO,
      queue_check_interval: Duration::from_secs(30),
      checksum_algo: None,
      skip_inbound_label_check: false,
      #[cfg(any(feature = "compression", feature = "encryption"))]
      offload_size: 1024 * 1024,
      #[cfg(feature = "encryption")]
      encryption_algo: None,
      #[cfg(feature = "encryption")]
      gossip_verify_incoming: false,
      #[cfg(feature = "encryption")]
      gossip_verify_outgoing: false,
      #[cfg(feature = "compression")]
      compress_algo: None,
      #[cfg(feature = "metrics")]
      metric_labels: std::sync::Arc::new(MetricLabels::new()),
    }
  }

  /// Returns a configuration
  /// that is optimized for most WAN environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn wan() -> Self {
    Self::lan()
      .with_timeout(Duration::from_secs(30))
      .with_suspicion_mult(6)
      .with_push_pull_interval(Duration::from_secs(60))
      .with_probe_timeout(Duration::from_secs(3))
      .with_probe_interval(Duration::from_secs(5))
      .with_gossip_nodes(4)
      .with_gossip_interval(Duration::from_millis(500))
      .with_gossip_to_the_dead_time(Duration::from_secs(60))
  }

  /// Returns a configuration
  /// that is optimized for a local loopback environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn local() -> Self {
    Self::lan()
      .with_timeout(Duration::from_secs(1))
      .with_indirect_checks(1)
      .with_retransmit_mult(2)
      .with_suspicion_mult(3)
      .with_push_pull_interval(Duration::from_secs(15))
      .with_probe_timeout(Duration::from_millis(200))
      .with_probe_interval(Duration::from_secs(1))
      .with_gossip_interval(Duration::from_millis(100))
      .with_gossip_to_the_dead_time(Duration::from_secs(15))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_constructor() {
    let _ = Options::wan();
    let _ = Options::lan();
    let _ = Options::local();
  }
}
