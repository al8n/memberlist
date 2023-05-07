use flutter_rust_bridge::IntoDart;
use flutter_rust_bridge::support::ffi::DartCObject;
use flutter_rust_bridge_macros::*;
use showbiz_core::{SecretKey, SmolStr, Options as ROptions};
use chrono::Duration;
use std::time::Duration as StdDuration;
use std::str::FromStr;

#[viewit::viewit(vis_all = "pub(crate)", getters(skip), setters(skip))]
#[frb]
#[derive(Debug, Clone)]
pub struct Options {
  /// The name of this node. This must be unique in the cluster.
  #[frb(non_final)]
  name: String,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  #[frb(default = "")]
  #[frb(non_final)]
  label: String,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  #[frb(default = false)]
  #[frb(non_final)]
  skip_inbound_label_check: bool,

  /// Configuration related to what address to bind to and ports to
  /// listen on. The port is used for both UDP and TCP gossip. It is
  /// assumed other nodes are running on this port, but they do not need
  /// to. Default is `0.0.0.0:7946`.
  #[frb(default = "0.0.0.0:7946")]
  #[frb(non_final)]
  bind_addr: String,

  /// Configuration related to what address to advertise to other
  /// cluster members. Used for nat traversal.
  #[frb(non_final)]
  advertise_addr: Option<String>,

  /// The configured protocol version that we
  /// will _speak_. This must be between [`MIN_PROTOCOL_VERSION`] and
  /// [`MAX_PROTOCOL_VERSION`].
  #[frb(default = 0)]
  #[frb(non_final)]
  protocol_version: u8,

  /// The timeout for establishing a stream connection with
  /// a remote node for a full state sync, and for stream read and write
  /// operations. This is a legacy name for backwards compatibility, but
  /// should really be called StreamDurationout now that we have generalized
  /// the transport.
  #[frb(non_final)]
  tcp_timeout: chrono::Duration,

  /// The number of nodes that will be asked to perform
  /// an indirect probe of a node in the case a direct probe fails. Memberlist
  /// waits for an ack from any single indirect node, so increasing this
  /// number will increase the likelihood that an indirect probe will succeed
  /// at the expense of bandwidth.
  #[frb(default = 3)]
  #[frb(non_final)]
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
  #[frb(default = 4)]
  #[frb(non_final)]
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
  #[frb(default = 4)]
  #[frb(non_final)]
  suspicion_mult: usize,

  /// The multiplier applied to the
  /// `suspicion_timeout` used as an upper bound on detection time. This max
  /// timeout is calculated using the formula:
  ///
  /// `suspicion_max_timeout = suspicion_max_timeout_mult * suspicion_timeout`
  ///
  /// If everything is working properly, confirmations from other nodes will
  /// accelerate suspicion timers in a manner which will cause the timeout
  /// to reach the base SuspicionDurationout before that elapses, so this value
  /// will typically only come into play if a node is experiencing issues
  /// communicating with other nodes. It should be set to a something fairly
  /// large so that a node having problems will have a lot of chances to
  /// recover before falsely declaring other nodes as failed, but short
  /// enough for a legitimately isolated node to still make progress marking
  /// nodes failed in a reasonable amount of time.
  #[frb(default = 6)]
  #[frb(non_final)]
  suspicion_max_timeout_mult: usize,

  /// The interval between complete state syncs.
  /// Complete state syncs are done with a single node over TCP and are
  /// quite expensive relative to standard gossiped messages. Setting this
  /// to zero will disable state push/pull syncs completely.
  ///
  /// Setting this interval lower (more frequent) will increase convergence
  /// speeds across larger clusters at the expense of increased bandwidth
  /// usage.
  // #[frb(default = "default_duration(10)")]
  #[frb(non_final)]
  push_pull_interval: Duration,

  /// The interval between random node probes. Setting
  /// this lower (more frequent) will cause the memberlist cluster to detect
  /// failed nodes more quickly at the expense of increased bandwidth usage
  #[frb(non_final)]
  probe_interval: Duration,
  /// The timeout to wait for an ack from a probed node
  /// before assuming it is unhealthy. This should be set to 99-percentile
  /// of RTT (round-trip time) on your network.
  #[frb(non_final)]
  probe_timeout: Duration,
  /// Set this field will turn off the fallback TCP pings that are attempted
  /// if the direct UDP ping fails. These get pipelined along with the
  /// indirect UDP pings.
  #[frb(default = false)]
  #[frb(non_final)]
  disable_tcp_pings: bool,

  /// Increase the probe interval if the node
  /// becomes aware that it might be degraded and not meeting the soft real
  /// time requirements to reliably probe other nodes.
  #[frb(default = 8)]
  #[frb(non_final)]
  awareness_max_multiplier: usize,
  /// The interval between sending messages that need
  /// to be gossiped that haven't been able to piggyback on probing messages.
  /// If this is set to zero, non-piggyback gossip is disabled. By lowering
  /// this value (more frequent) gossip messages are propagated across
  /// the cluster more quickly at the expense of increased bandwidth.
  #[frb(non_final)]
  gossip_interval: Duration,
  /// The number of random nodes to send gossip messages to
  /// per `gossip_interval`. Increasing this number causes the gossip messages
  /// to propagate across the cluster more quickly at the expense of
  /// increased bandwidth.
  #[frb(default = 3)]
  #[frb(non_final)]
  gossip_nodes: usize,
  /// The interval after which a node has died that
  /// we will still try to gossip to it. This gives it a chance to refute.
  #[frb(non_final)]
  gossip_to_the_dead_time: Duration,
  /// Controls whether to enforce encryption for incoming
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  #[frb(default = true)]
  #[frb(non_final)]
  gossip_verify_incoming: bool,
  /// Controls whether to enforce encryption for outgoing
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  #[frb(default = true)]
  #[frb(non_final)]
  gossip_verify_outgoing: bool,
  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization. This is only available starting at protocol version 1.
  #[frb(default = true)]
  #[frb(non_final)]
  enable_compression: bool,

  /// Used to initialize the primary encryption key in a keyring.
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  /// The value should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[frb(non_final)]
  secret_key: Option<Vec<u8>>,

  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  #[frb(default = 0)]
  #[frb(non_final)]
  delegate_protocol_version: u8,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  #[frb(default = 0)]
  #[frb(non_final)]
  delegate_protocol_min: u8,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  #[frb(default = 0)]
  #[frb(non_final)]
  delegate_protocol_max: u8,

  /// Points to the system's DNS config file, usually located
  /// at `/etc/resolv.conf`. It can be overridden via config for easier testing.
  #[frb(default = "/etc/resolv.conf")]
  #[frb(non_final)]
  dns_config_path: String,

  /// Size of Memberlist's internal channel which handles UDP messages. The
  /// size of this determines the size of the queue which Memberlist will keep
  /// while UDP messages are handled.
  #[frb(default = 1024)]
  #[frb(non_final)]
  handoff_queue_depth: usize,
  /// Maximum number of bytes that memberlist will put in a packet (this
  /// will be for UDP packets by default with a NetTransport). A safe value
  /// for this is typically 1400 bytes (which is the default). However,
  /// depending on your network's MTU (Maximum Transmission Unit) you may
  /// be able to increase this to get more content into each gossip packet.
  #[frb(default = 1400)]
  #[frb(non_final)]
  packet_buffer_size: usize,

  /// Controls the time before a dead node's name can be
  /// reclaimed by one with a different address or port. By default, this is 0,
  /// meaning nodes cannot be reclaimed this way.
  #[frb(non_final)]
  dead_node_reclaim_time: Duration,

  /// Controls if the name of a node is required when sending
  /// a message to that node.
  #[frb(default = false)]
  #[frb(non_final)]
  require_node_names: bool,

  /// If [`None`], allow any connection (default), otherwise specify all networks
  /// allowed to connect (you must specify IPv6/IPv4 separately)
  /// Using an empty Vec will block all connections.
  #[frb(non_final)]
  allowed_cidrs: Option<Vec<String>>,
  /// The interval at which we check the message
  /// queue to apply the warning and max depth.
  #[frb(non_final)]
  queue_check_interval: Duration,
}

impl Options {
  pub fn lan() -> Options {
    Self::from(ROptions::lan())
  }

  pub fn wan() -> Options {
    Self::from(ROptions::wan())
  }

  pub fn local() -> Options {
    Self::from(ROptions::local())
  }
}

impl TryFrom<Options> for ROptions {
  type Error = String;

  fn try_from(value: Options) -> Result<Self, Self::Error> {
    Ok(Self {
      name: value.name.into(),
      label: value.label.into(),
      skip_inbound_label_check: value.skip_inbound_label_check,
      bind_addr: value
        .bind_addr
        .parse::<std::net::SocketAddr>()
        .map_err(|e| e.to_string())?,
      advertise_addr: value
        .advertise_addr
        .map(|x| x.parse::<std::net::SocketAddr>().map_err(|e| e.to_string()))
        .transpose()?,
      protocol_version: value.protocol_version,
      tcp_timeout: StdDuration::from_millis(value.tcp_timeout.num_milliseconds() as u64),
      indirect_checks: value.indirect_checks,
      retransmit_mult: value.retransmit_mult,
      suspicion_mult: value.suspicion_mult,
      suspicion_max_timeout_mult: value.suspicion_max_timeout_mult,
      push_pull_interval: StdDuration::from_millis(value.push_pull_interval.num_milliseconds() as u64),
      probe_interval: StdDuration::from_millis(value.probe_interval.num_milliseconds() as u64),
      probe_timeout: StdDuration::from_millis(value.probe_timeout.num_milliseconds() as u64),
      disable_tcp_pings: value.disable_tcp_pings,
      awareness_max_multiplier: value.awareness_max_multiplier,
      gossip_interval: StdDuration::from_millis(value.gossip_interval.num_milliseconds() as u64),
      gossip_nodes: value.gossip_nodes,
      gossip_to_the_dead_time: StdDuration::from_millis(value.gossip_to_the_dead_time.num_milliseconds() as u64),
      gossip_verify_incoming: value.gossip_verify_incoming,
      gossip_verify_outgoing: value.gossip_verify_outgoing,
      enable_compression: value.enable_compression,
      secret_key: value
        .secret_key
        .map(|x| SecretKey::try_from(x.as_slice()))
        .transpose()?,
      delegate_protocol_version: value.delegate_protocol_version,
      delegate_protocol_min: value.delegate_protocol_min,
      delegate_protocol_max: value.delegate_protocol_max,
      dns_config_path: std::path::PathBuf::from(value.dns_config_path),
      handoff_queue_depth: value.handoff_queue_depth,
      packet_buffer_size: value.packet_buffer_size,
      dead_node_reclaim_time: StdDuration::from_millis(value.dead_node_reclaim_time.num_milliseconds() as u64),
      require_node_names: value.require_node_names,
      allowed_cidrs: value
        .allowed_cidrs
        .map(|x| {
          x.into_iter()
            .map(|x| ipnet::IpNet::from_str(x.as_str()).map_err(|e| e.to_string()))
            .collect::<Result<std::collections::HashSet<_>, _>>()
        })
        .transpose()?,
      queue_check_interval: StdDuration::from_millis(value.queue_check_interval.num_milliseconds() as u64),
    })
  }
}

impl From<ROptions> for Options {
  fn from(value: ROptions) -> Self {
    Self {
      name: value.name.into(),
      label: value.label.into(),
      skip_inbound_label_check: value.skip_inbound_label_check,
      bind_addr: value.bind_addr.to_string(),
      advertise_addr: value.advertise_addr.map(|x| x.to_string()),
      protocol_version: value.protocol_version,
      tcp_timeout: Duration::from_std(value.tcp_timeout).unwrap(),
      indirect_checks: value.indirect_checks,
      retransmit_mult: value.retransmit_mult,
      suspicion_mult: value.suspicion_mult,
      suspicion_max_timeout_mult: value.suspicion_max_timeout_mult,
      push_pull_interval: Duration::from_std(value.push_pull_interval).unwrap(),
      probe_interval: Duration::from_std(value.probe_interval).unwrap(),
      probe_timeout: Duration::from_std(value.probe_timeout).unwrap(),
      disable_tcp_pings: value.disable_tcp_pings,
      awareness_max_multiplier: value.awareness_max_multiplier,
      gossip_interval: Duration::from_std(value.gossip_interval).unwrap(),
      gossip_nodes: value.gossip_nodes,
      gossip_to_the_dead_time: Duration::from_std(value.gossip_to_the_dead_time).unwrap(),
      gossip_verify_incoming: value.gossip_verify_incoming,
      gossip_verify_outgoing: value.gossip_verify_outgoing,
      enable_compression: value.enable_compression,
      secret_key: value.secret_key.map(|x| x.to_vec()),
      delegate_protocol_version: value.delegate_protocol_version,
      delegate_protocol_min: value.delegate_protocol_min,
      delegate_protocol_max: value.delegate_protocol_max,
      dns_config_path: value.dns_config_path.to_string_lossy().to_string(),
      handoff_queue_depth: value.handoff_queue_depth,
      packet_buffer_size: value.packet_buffer_size,
      dead_node_reclaim_time: Duration::from_std(value.dead_node_reclaim_time).unwrap(),
      require_node_names: value.require_node_names,
      allowed_cidrs: value
        .allowed_cidrs
        .map(|x| x.into_iter().map(|x| x.to_string()).collect()),
      queue_check_interval: Duration::from_std(value.queue_check_interval).unwrap(),
    }
  }
}