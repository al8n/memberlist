use showbiz_core::{Options as ROptions, SecretKey};
use std::{str::FromStr, time::Duration as StdDuration};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[wasm_bindgen]
pub struct Options {
  /// The name of this node. This must be unique in the cluster.
  name: String,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  label: String,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  skip_inbound_label_check: bool,

  /// Configuration related to what address to bind to and ports to
  /// listen on. The port is used for both UDP and TCP gossip. It is
  /// assumed other nodes are running on this port, but they do not need
  /// to.
  bind_addr: String,

  /// Configuration related to what address to advertise to other
  /// cluster members. Used for nat traversal.
  advertise_addr: Option<String>,

  /// The configured protocol version that we
  /// will _speak_. This must be between [`MIN_PROTOCOL_VERSION`] and
  /// [`MAX_PROTOCOL_VERSION`].
  protocol_version: u8,

  /// The timeout for establishing a stream connection with
  /// a remote node for a full state sync, and for stream read and write
  /// operations. This is a legacy name for backwards compatibility, but
  /// should really be called StreamTimeout now that we have generalized
  /// the transport.
  tcp_timeout: Duration,

  /// The number of nodes that will be asked to perform
  /// an indirect probe of a node in the case a direct probe fails. Memberlist
  /// waits for an ack from any single indirect node, so increasing this
  /// number will increase the likelihood that an indirect probe will succeed
  /// at the expense of bandwidth.
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
  suspicion_max_timeout_mult: usize,

  /// The interval between complete state syncs.
  /// Complete state syncs are done with a single node over TCP and are
  /// quite expensive relative to standard gossiped messages. Setting this
  /// to zero will disable state push/pull syncs completely.
  ///
  /// Setting this interval lower (more frequent) will increase convergence
  /// speeds across larger clusters at the expense of increased bandwidth
  /// usage.
  push_pull_interval: Duration,

  /// The interval between random node probes. Setting
  /// this lower (more frequent) will cause the memberlist cluster to detect
  /// failed nodes more quickly at the expense of increased bandwidth usage
  probe_interval: Duration,
  /// The timeout to wait for an ack from a probed node
  /// before assuming it is unhealthy. This should be set to 99-percentile
  /// of RTT (round-trip time) on your network.
  probe_timeout: Duration,
  /// Set this field will turn off the fallback TCP pings that are attempted
  /// if the direct UDP ping fails. These get pipelined along with the
  /// indirect UDP pings.
  disable_tcp_pings: bool,

  /// Increase the probe interval if the node
  /// becomes aware that it might be degraded and not meeting the soft real
  /// time requirements to reliably probe other nodes.
  awareness_max_multiplier: usize,
  /// The interval between sending messages that need
  /// to be gossiped that haven't been able to piggyback on probing messages.
  /// If this is set to zero, non-piggyback gossip is disabled. By lowering
  /// this value (more frequent) gossip messages are propagated across
  /// the cluster more quickly at the expense of increased bandwidth.
  gossip_interval: Duration,
  /// The number of random nodes to send gossip messages to
  /// per `gossip_interval`. Increasing this number causes the gossip messages
  /// to propagate across the cluster more quickly at the expense of
  /// increased bandwidth.
  gossip_nodes: usize,
  /// The interval after which a node has died that
  /// we will still try to gossip to it. This gives it a chance to refute.
  gossip_to_the_dead_time: Duration,
  /// Controls whether to enforce encryption for incoming
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  gossip_verify_incoming: bool,
  /// Controls whether to enforce encryption for outgoing
  /// gossip. It is used for upshifting from unencrypted to encrypted gossip on
  /// a running cluster.
  gossip_verify_outgoing: bool,
  /// Used to control message compression. This can
  /// be used to reduce bandwidth usage at the cost of slightly more CPU
  /// utilization. This is only available starting at protocol version 1.
  enable_compression: bool,

  /// Used to initialize the primary encryption key in a keyring.
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  /// The value should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  secret_key: Option<Vec<u8>>,

  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_protocol_version: u8,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_protocol_min: u8,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_protocol_max: u8,

  /// Points to the system's Dns config file, usually located
  /// at `/etc/resolv.conf`. It can be overridden via config for easier testing.
  dns_config_path: String,

  /// Size of Memberlist's internal channel which handles UDP messages. The
  /// size of this determines the size of the queue which Memberlist will keep
  /// while UDP messages are handled.
  handoff_queue_depth: usize,
  /// Maximum number of bytes that memberlist will put in a packet (this
  /// will be for UDP packets by default with a NetTransport). A safe value
  /// for this is typically 1400 bytes (which is the default). However,
  /// depending on your network's MTU (Maximum Transmission Unit) you may
  /// be able to increase this to get more content into each gossip packet.
  packet_buffer_size: usize,

  /// Controls the time before a dead node's name can be
  /// reclaimed by one with a different address or port. By default, this is 0,
  /// meaning nodes cannot be reclaimed this way.
  dead_node_reclaim_time: Duration,

  /// Controls if the name of a node is required when sending
  /// a message to that node.
  require_node_names: bool,

  /// If [`None`], allow any connection (default), otherwise specify all networks
  /// allowed to connect (you must specify IPv6/IPv4 separately)
  /// Using an empty Vec will block all connections.
  allowed_cidrs: Option<Vec<String>>,
  /// The interval at which we check the message
  /// queue to apply the warning and max depth.
  queue_check_interval: Duration,
}

#[wasm_bindgen]
impl Options {
  #[inline]
  pub fn name(&self) -> String {
    self.name.clone()
  }
  #[inline]
  pub fn label(&self) -> String {
    self.label.clone()
  }
  #[inline]
  pub fn skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
  #[inline]
  pub fn bind_addr(&self) -> String {
    self.bind_addr.clone()
  }
  #[inline]
  pub fn advertise_addr(&self) -> Option<String> {
    self.advertise_addr.clone()
  }
  #[inline]
  pub fn protocol_version(&self) -> u8 {
    self.protocol_version
  }
  #[inline]
  pub fn tcp_timeout(&self) -> Duration {
    self.tcp_timeout
  }
  #[inline]
  pub fn indirect_checks(&self) -> usize {
    self.indirect_checks
  }
  #[inline]
  pub fn retransmit_mult(&self) -> usize {
    self.retransmit_mult
  }
  #[inline]
  pub fn suspicion_mult(&self) -> usize {
    self.suspicion_mult
  }
  #[inline]
  pub fn suspicion_max_timeout_mult(&self) -> usize {
    self.suspicion_max_timeout_mult
  }
  #[inline]
  pub fn push_pull_interval(&self) -> Duration {
    self.push_pull_interval
  }
  #[inline]
  pub fn probe_interval(&self) -> Duration {
    self.probe_interval
  }
  #[inline]
  pub fn probe_timeout(&self) -> Duration {
    self.probe_timeout
  }
  #[inline]
  pub fn disable_tcp_pings(&self) -> bool {
    self.disable_tcp_pings
  }
  #[inline]
  pub fn awareness_max_multiplier(&self) -> usize {
    self.awareness_max_multiplier
  }
  #[inline]
  pub fn gossip_interval(&self) -> Duration {
    self.gossip_interval
  }
  #[inline]
  pub fn gossip_nodes(&self) -> usize {
    self.gossip_nodes
  }
  #[inline]
  pub fn gossip_to_the_dead_time(&self) -> Duration {
    self.gossip_to_the_dead_time
  }
  #[inline]
  pub fn gossip_verify_incoming(&self) -> bool {
    self.gossip_verify_incoming
  }
  #[inline]
  pub fn gossip_verify_outgoing(&self) -> bool {
    self.gossip_verify_outgoing
  }
  #[inline]
  pub fn enable_compression(&self) -> bool {
    self.enable_compression
  }
  #[inline]
  pub fn secret_key(&self) -> Option<Vec<u8>> {
    self.secret_key.clone().map(|x| x.to_vec())
  }
  #[inline]
  pub fn delegate_protocol_version(&self) -> u8 {
    self.delegate_protocol_version
  }
  #[inline]
  pub fn delegate_protocol_min(&self) -> u8 {
    self.delegate_protocol_min
  }
  #[inline]
  pub fn delegate_protocol_max(&self) -> u8 {
    self.delegate_protocol_max
  }
  #[inline]
  pub fn dns_config_path(&self) -> String {
    self.dns_config_path.clone()
  }
  #[inline]
  pub fn handoff_queue_depth(&self) -> usize {
    self.handoff_queue_depth
  }
  #[inline]
  pub fn packet_buffer_size(&self) -> usize {
    self.packet_buffer_size
  }
  #[inline]
  pub fn dead_node_reclaim_time(&self) -> Duration {
    self.dead_node_reclaim_time
  }
  #[inline]
  pub fn require_node_names(&self) -> bool {
    self.require_node_names
  }
  #[inline]
  pub fn allowed_cidrs(&self) -> JsValue {
    match &self.allowed_cidrs {
      Some(cidrs) => {
        let js_cidrs = cidrs
          .iter()
          .map(|x| JsValue::from_str(x))
          .collect::<js_sys::Array>();
        JsValue::from(js_cidrs)
      }
      None => JsValue::NULL,
    }
  }
  #[inline]
  pub fn queue_check_interval(&self) -> Duration {
    self.queue_check_interval
  }
  #[inline]
  pub fn with_name(mut self, val: String) -> Self {
    self.name = val;
    self
  }
  #[inline]
  pub fn with_label(mut self, val: String) -> Self {
    self.label = val;
    self
  }
  #[inline]
  pub fn with_skip_inbound_label_check(mut self, val: bool) -> Self {
    self.skip_inbound_label_check = val;
    self
  }
  #[inline]
  pub fn with_bind_addr(mut self, val: String) -> Self {
    self.bind_addr = val;
    self
  }
  #[inline]
  pub fn with_advertise_addr(mut self, val: Option<String>) -> Self {
    self.advertise_addr = val;
    self
  }
  #[inline]
  pub fn with_protocol_version(mut self, val: u8) -> Self {
    self.protocol_version = val;
    self
  }
  #[inline]
  pub fn with_tcp_timeout(mut self, val: Duration) -> Self {
    self.tcp_timeout = val;
    self
  }
  #[inline]
  pub fn with_indirect_checks(mut self, val: usize) -> Self {
    self.indirect_checks = val;
    self
  }
  #[inline]
  pub fn with_retransmit_mult(mut self, val: usize) -> Self {
    self.retransmit_mult = val;
    self
  }
  #[inline]
  pub fn with_suspicion_mult(mut self, val: usize) -> Self {
    self.suspicion_mult = val;
    self
  }
  #[inline]
  pub fn with_suspicion_max_timeout_mult(mut self, val: usize) -> Self {
    self.suspicion_max_timeout_mult = val;
    self
  }
  #[inline]
  pub fn with_push_pull_interval(mut self, val: Duration) -> Self {
    self.push_pull_interval = val;
    self
  }
  #[inline]
  pub fn with_probe_interval(mut self, val: Duration) -> Self {
    self.probe_interval = val;
    self
  }
  #[inline]
  pub fn with_probe_timeout(mut self, val: Duration) -> Self {
    self.probe_timeout = val;
    self
  }
  #[inline]
  pub fn with_disable_tcp_pings(mut self, val: bool) -> Self {
    self.disable_tcp_pings = val;
    self
  }
  #[inline]
  pub fn with_awareness_max_multiplier(mut self, val: usize) -> Self {
    self.awareness_max_multiplier = val;
    self
  }
  #[inline]
  pub fn with_gossip_interval(mut self, val: Duration) -> Self {
    self.gossip_interval = val;
    self
  }
  #[inline]
  pub fn with_gossip_nodes(mut self, val: usize) -> Self {
    self.gossip_nodes = val;
    self
  }
  #[inline]
  pub fn with_gossip_to_the_dead_time(mut self, val: Duration) -> Self {
    self.gossip_to_the_dead_time = val;
    self
  }
  #[inline]
  pub fn with_gossip_verify_incoming(mut self, val: bool) -> Self {
    self.gossip_verify_incoming = val;
    self
  }
  #[inline]
  pub fn with_gossip_verify_outgoing(mut self, val: bool) -> Self {
    self.gossip_verify_outgoing = val;
    self
  }
  #[inline]
  pub fn with_enable_compression(mut self, val: bool) -> Self {
    self.enable_compression = val;
    self
  }
  #[inline]
  pub fn with_secret_key(mut self, val: Option<Vec<u8>>) -> Self {
    self.secret_key = val;
    self
  }
  #[inline]
  pub fn with_delegate_protocol_version(mut self, val: u8) -> Self {
    self.delegate_protocol_version = val;
    self
  }
  #[inline]
  pub fn with_delegate_protocol_min(mut self, val: u8) -> Self {
    self.delegate_protocol_min = val;
    self
  }
  #[inline]
  pub fn with_delegate_protocol_max(mut self, val: u8) -> Self {
    self.delegate_protocol_max = val;
    self
  }
  #[inline]
  pub fn with_dns_config_path(mut self, val: String) -> Self {
    self.dns_config_path = val;
    self
  }
  #[inline]
  pub fn with_handoff_queue_depth(mut self, val: usize) -> Self {
    self.handoff_queue_depth = val;
    self
  }
  #[inline]
  pub fn with_packet_buffer_size(mut self, val: usize) -> Self {
    self.packet_buffer_size = val;
    self
  }
  #[inline]
  pub fn with_dead_node_reclaim_time(mut self, val: Duration) -> Self {
    self.dead_node_reclaim_time = val;
    self
  }
  #[inline]
  pub fn with_require_node_names(mut self, val: bool) -> Self {
    self.require_node_names = val;
    self
  }
  #[inline]
  pub fn add_allowed_cidr(mut self, val: String) -> Self {
    match &mut self.allowed_cidrs {
      Some(cidrs) => cidrs.push(val),
      None => self.allowed_cidrs = Some(vec![val]),
    }
    self
  }
  #[inline]
  pub fn clear_allowed_cidrs(mut self) -> Self {
    self.allowed_cidrs = None;
    self
  }
  #[inline]
  pub fn with_queue_check_interval(mut self, val: Duration) -> Self {
    self.queue_check_interval = val;
    self
  }

  /// Returns a sane set of configurations for Memberlist.
  /// It uses the hostname as the node name, and otherwise sets very conservative
  /// values that are sane for most LAN environments. The default configuration
  /// errs on the side of caution, choosing values that are optimized
  /// for higher convergence at the cost of higher bandwidth usage. Regardless,
  /// these values are a good starting point when getting started with memberlist.
  #[inline]
  pub fn lan() -> Self {
    Self::from(ROptions::lan())
  }

  /// Returns a configuration
  /// that is optimized for most WAN environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn wan() -> Self {
    Self::from(ROptions::wan())
  }

  /// Returns a configuration
  /// that is optimized for a local loopback environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn local() -> Self {
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
      tcp_timeout: value.tcp_timeout.into(),
      indirect_checks: value.indirect_checks,
      retransmit_mult: value.retransmit_mult,
      suspicion_mult: value.suspicion_mult,
      suspicion_max_timeout_mult: value.suspicion_max_timeout_mult,
      push_pull_interval: value.push_pull_interval.into(),
      probe_interval: value.probe_interval.into(),
      probe_timeout: value.probe_timeout.into(),
      disable_tcp_pings: value.disable_tcp_pings,
      awareness_max_multiplier: value.awareness_max_multiplier,
      gossip_interval: value.gossip_interval.into(),
      gossip_nodes: value.gossip_nodes,
      gossip_to_the_dead_time: value.gossip_to_the_dead_time.into(),
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
      dead_node_reclaim_time: value.dead_node_reclaim_time.into(),
      require_node_names: value.require_node_names,
      allowed_cidrs: value
        .allowed_cidrs
        .map(|x| {
          x.into_iter()
            .map(|x| ipnet::IpNet::from_str(x.as_str()).map_err(|e| e.to_string()))
            .collect::<Result<std::collections::HashSet<_>, _>>()
        })
        .transpose()?,
      queue_check_interval: value.queue_check_interval.into(),
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
      tcp_timeout: value.tcp_timeout.into(),
      indirect_checks: value.indirect_checks,
      retransmit_mult: value.retransmit_mult,
      suspicion_mult: value.suspicion_mult,
      suspicion_max_timeout_mult: value.suspicion_max_timeout_mult,
      push_pull_interval: value.push_pull_interval.into(),
      probe_interval: value.probe_interval.into(),
      probe_timeout: value.probe_timeout.into(),
      disable_tcp_pings: value.disable_tcp_pings,
      awareness_max_multiplier: value.awareness_max_multiplier,
      gossip_interval: value.gossip_interval.into(),
      gossip_nodes: value.gossip_nodes,
      gossip_to_the_dead_time: value.gossip_to_the_dead_time.into(),
      gossip_verify_incoming: value.gossip_verify_incoming,
      gossip_verify_outgoing: value.gossip_verify_outgoing,
      enable_compression: value.enable_compression,
      secret_key: value.secret_key.map(|x| x.to_vec()),
      delegate_protocol_version: value.delegate_protocol_version,
      delegate_protocol_min: value.delegate_protocol_min,
      delegate_protocol_max: value.delegate_protocol_max,
      dns_config_path: format!("{}", value.dns_config_path.display()),
      handoff_queue_depth: value.handoff_queue_depth,
      packet_buffer_size: value.packet_buffer_size,
      dead_node_reclaim_time: value.dead_node_reclaim_time.into(),
      require_node_names: value.require_node_names,
      allowed_cidrs: value
        .allowed_cidrs
        .map(|x| x.into_iter().map(|x| x.to_string()).collect()),
      queue_check_interval: value.queue_check_interval.into(),
    }
  }
}

/// A `Duration` type to represent a span of time, typically used for system
/// timeouts.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[wasm_bindgen]
pub struct Duration {
  duration: StdDuration,
}

// Implement the conversion from StdDuration to Duration
impl From<StdDuration> for Duration {
  fn from(duration: StdDuration) -> Self {
    Duration { duration }
  }
}

impl From<Duration> for StdDuration {
  fn from(wasm_duration: Duration) -> Self {
    wasm_duration.duration
  }
}

#[wasm_bindgen]
impl Duration {
  pub fn zero() -> Self {
    Self {
      duration: StdDuration::ZERO,
    }
  }

  pub fn millisecond() -> Self {
    Self {
      duration: StdDuration::from_millis(1),
    }
  }

  pub fn second() -> Self {
    Self {
      duration: StdDuration::from_secs(1),
    }
  }

  pub fn minute() -> Self {
    Self {
      duration: StdDuration::from_secs(60),
    }
  }

  pub fn hour() -> Self {
    Self {
      duration: StdDuration::from_secs(3600),
    }
  }

  pub fn day() -> Self {
    Self {
      duration: StdDuration::from_secs(86400),
    }
  }

  pub fn week() -> Self {
    Self {
      duration: StdDuration::from_secs(604800),
    }
  }

  pub fn from_millis(millis: u64) -> Self {
    Self {
      duration: StdDuration::from_millis(millis),
    }
  }

  pub fn from_secs(secs: u64) -> Self {
    Self {
      duration: StdDuration::from_secs(secs),
    }
  }

  pub fn as_millis(&self) -> u64 {
    self.duration.as_millis() as u64
  }

  pub fn as_secs(&self) -> u64 {
    self.duration.as_secs()
  }
}
