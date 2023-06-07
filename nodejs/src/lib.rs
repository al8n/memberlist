#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use napi::{Error, Result, Status};

use showbiz_core::bytes::Bytes;
use showbiz_core::{IpNet, Options as ROptions, SecretKey, SmolStr};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration as StdDuration;

/// A `Duration` type to represent a span of time, typically used for system
/// timeouts.
///
/// Each `Duration` is composed of a whole number of seconds and a fractional part
/// represented in nanoseconds. If the underlying system does not support
/// nanosecond-level precision, APIs binding a system timeout will typically round up
/// the number of nanoseconds.
#[napi]
#[derive(Debug, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Duration(StdDuration);

#[napi]
impl Duration {
  /// Creates a new duration from the specified number of milliseconds.
  /// If the number less than 0, the duration will be 0.
  #[napi(constructor, writable = false)]
  pub fn new(millis: i64) -> Self {
    let millis = millis.max(0);
    Self(StdDuration::from_millis(millis as u64))
  }

  #[napi(factory, writable = false)]
  pub fn zero() -> Self {
    Self(StdDuration::ZERO)
  }

  /// Creates a new duration from the specified number of milliseconds.
  /// If the number less than 0, the duration will be 0.
  #[napi(factory, writable = false)]
  pub fn from_millis(millis: i64) -> Self {
    let millis = millis.max(0);
    Self(StdDuration::from_millis(millis as u64))
  }

  /// Creates a new duration from the specified number of seconds.
  /// If the number less than 0, the duration will be 0.
  #[napi(factory, writable = false)]
  pub fn from_secs(secs: i64) -> Self {
    let secs = secs.max(0);
    Self(StdDuration::from_secs(secs as u64))
  }

  /// Returns the total number of whole milliseconds contained by this `Duration`.
  #[napi(writable = false)]
  pub fn as_millis(&self) -> i64 {
    self.0.as_millis() as i64
  }

  /// Returns the number of _whole_ seconds contained by this `Duration`.
  ///
  /// The returned value does not include the fractional (nanosecond) part of the
  /// duration.
  #[napi(writable = false)]
  pub fn as_secs(&self) -> i64 {
    self.0.as_secs() as i64
  }

  /// Returns true if this `Duration` spans no time.
  #[napi(writable = false)]
  pub fn is_zero(&self) -> bool {
    self.0.is_zero()
  }
}

impl From<StdDuration> for Duration {
  fn from(d: StdDuration) -> Self {
    Self(d)
  }
}

impl From<Duration> for StdDuration {
  fn from(d: Duration) -> Self {
    d.0
  }
}

/// Options for configuring a memberlist.
///
/// The default configuration is optimized for most WAN environments.
///
/// # Factory Methods
///
/// - `lan`:
///
///   Returns a sane set of configurations for Memberlist.
///   It uses the hostname as the node name, and otherwise sets very conservative
///   values that are sane for most LAN environments. The default configuration
///   errs on the side of caution, choosing values that are optimized
///   for higher convergence at the cost of higher bandwidth usage. Regardless,
///   these values are a good starting point when getting started with memberlist.
///
/// - `wan`:
///   
///   Returns a configuration
///   that is optimized for most WAN environments. The default configuration is
///   still very conservative and errs on the side of caution.
///
/// - `local`:
///   
///   Returns a configuration
///   that is optimized for a local loopback environments. The default configuration is
///   still very conservative and errs on the side of caution.
///
/// - `fromJSON`:
///
///   Deserializes the options from JSON string. The JSON string must be a serialized by `toJSON`.
///
/// - `fromYAML`:
///   
///   Deserializes the options from YAML string. The YAML string must be a serialized by `toYAML`.
///
/// # Avaliable Settings
///
/// - **name**
///   
///   The name of this node. This must be unique in the cluster.
///
/// - **label**
///
///   Label is an optional set of bytes to include on the outside of each
///   packet and stream.
///   If gossip encryption is enabled and this is set it is treated as GCM
///   authenticated data.
///
/// - **skipInboundLabelCheck**
///   
///   Skips the check that inbound packets and gossip
///   streams need to be label prefixed.
///
/// - **bindAddr**
///
///   Configuration related to what address to bind to and ports to
///   listen on. The port is used for both UDP and TCP gossip. It is
///   assumed other nodes are running on this port, but they do not need
///   to.
///
/// - **advertiseAddr**
///
///   Configuration related to what address to advertise to other
///   cluster members. Used for nat traversal.
///
/// - **protocolVersion**
///   
///   The configured protocol version that we
///   will _speak_. This must be between [`MIN_PROTOCOL_VERSION`] and
///   [`MAX_PROTOCOL_VERSION`].
///
/// - **tcpTimeout**
///
///   The timeout for establishing a stream connection with
///   a remote node for a full state sync, and for stream read and write
///   operations. This is a legacy name for backwards compatibility, but
///   should really be called StreamTimeout now that we have generalized
///   the transport.
///
/// - **indirectChecks**
///   
///   The number of nodes that will be asked to perform
///   an indirect probe of a node in the case a direct probe fails. Memberlist
///   waits for an ack from any single indirect node, so increasing this
///   number will increase the likelihood that an indirect probe will succeed
///   at the expense of bandwidth.
///
/// - **retransmitMult**
///
///   The multiplier for the number of retransmissions
///   that are attempted for messages broadcasted over gossip. The actual
///   count of retransmissions is calculated using the formula:
///  
///     `retransmits = retransmitMult * log(N+1)`
///  
///   This allows the retransmits to scale properly with cluster size. The
///   higher the multiplier, the more likely a failed broadcast is to converge
///   at the expense of increased bandwidth.
///
/// - **suspicionMult**
///   
///   The multiplier for determining the time an
///   inaccessible node is considered suspect before declaring it dead.
///   The actual timeout is calculated using the formula:
///  
///     `suspicionTimeout = suspicionMult * log(N+1) * probeInterval`
///  
///   This allows the timeout to scale properly with expected propagation
///   delay with a larger cluster size. The higher the multiplier, the longer
///   an inaccessible node is considered part of the cluster before declaring
///   it dead, giving that suspect node more time to refute if it is indeed
///   still alive.
///
/// - **suspicionMaxTimeoutMult**
///
///   The multiplier applied to the
///   `suspicion_timeout` used as an upper bound on detection time. This max
///   timeout is calculated using the formula:
///  
///   `suspicionMaxTimeout = suspicionMaxTimeoutMult * suspicionTimeout`
///  
///   If everything is working properly, confirmations from other nodes will
///   accelerate suspicion timers in a manner which will cause the timeout
///   to reach the base SuspicionTimeout before that elapses, so this value
///   will typically only come into play if a node is experiencing issues
///   communicating with other nodes. It should be set to a something fairly
///   large so that a node having problems will have a lot of chances to
///   recover before falsely declaring other nodes as failed, but short
///   enough for a legitimately isolated node to still make progress marking
///   nodes failed in a reasonable amount of time.
///
/// - **pushPullInterval**
///
///   The interval between complete state syncs.
///   Complete state syncs are done with a single node over TCP and are
///   quite expensive relative to standard gossiped messages. Setting this
///   to zero will disable state push/pull syncs completely.
///  
///   Setting this interval lower (more frequent) will increase convergence
///   speeds across larger clusters at the expense of increased bandwidth
///   usage.
///
/// - **probeInterval**
///
///   The interval between random node probes. Setting
///   this lower (more frequent) will cause the memberlist cluster to detect
///   failed nodes more quickly at the expense of increased bandwidth usage
///
/// - **probeTimeout**
///
///   The timeout to wait for an ack from a probed node
///   before assuming it is unhealthy. This should be set to 99-percentile
///   of RTT (round-trip time) on your network.
///
/// - **disableTcpPings**
///
///   Set this field will turn off the fallback TCP pings that are attempted
///   if the direct UDP ping fails. These get pipelined along with the
///   indirect UDP pings.
///
/// - **awarenessMaxMultiplier**
///
///   Increase the probe interval if the node
///   becomes aware that it might be degraded and not meeting the soft real
///   time requirements to reliably probe other nodes.
///
/// - **gossipInterval**
///
///   The interval between sending messages that need
///   to be gossiped that haven't been able to piggyback on probing messages.
///   If this is set to zero, non-piggyback gossip is disabled. By lowering
///   this value (more frequent) gossip messages are propagated across
///   the cluster more quickly at the expense of increased bandwidth.
///
/// - **gossipNodes**
///
///   The number of random nodes to send gossip messages to
///   per `gossip_interval`. Increasing this number causes the gossip messages
///   to propagate across the cluster more quickly at the expense of
///   increased bandwidth.
///
/// - **gossipToTheDeadTime**
///
///   The interval after which a node has died that
///   we will still try to gossip to it. This gives it a chance to refute.
///
/// - **gossipVerifyIncoming**
///
///   Controls whether to enforce encryption for incoming
///   gossip. It is used for upshifting from unencrypted to encrypted gossip on
///   a running cluster.
///
/// - **gossipVerifyOutgoing**
///
///   Controls whether to enforce encryption for outgoing
///   gossip. It is used for upshifting from unencrypted to encrypted gossip on
///   a running cluster.
///
/// - **enableCompression**
///
///   Used to control message compression. This can
///   be used to reduce bandwidth usage at the cost of slightly more CPU
///   utilization. This is only available starting at protocol version 1.
///
/// - **secretKey**
///
///   Used to initialize the primary encryption key in a keyring.
///   The primary encryption key is the only key used to encrypt messages and
///   the first key used while attempting to decrypt messages. Providing a
///   value for this primary key will enable message-level encryption and
///   verification, and automatically install the key onto the keyring.
///   The value should be either 16, 24, or 32 bytes to select AES-128,
///   AES-192, or AES-256.
///
/// - **delegateProtocolVersion**
///
///   Used to guarantee protocol-compatibility
///   for any custom messages that the delegate might do (broadcasts,
///   local/remote state, etc.). If you don't set these, then the protocol
///   versions will just be zero, and version compliance won't be done.
///
/// - **delegateProtocolMin**
///
///   Used to guarantee protocol-compatibility
///   for any custom messages that the delegate might do (broadcasts,
///   local/remote state, etc.). If you don't set these, then the protocol
///   versions will just be zero, and version compliance won't be done.
///
/// - **delegateProtocolMax**
///
///   Used to guarantee protocol-compatibility
///   for any custom messages that the delegate might do (broadcasts,
///   local/remote state, etc.). If you don't set these, then the protocol
///   versions will just be zero, and version compliance won't be done.
///
/// - **dnsConfigPath**
///
///   Points to the system's Dns config file, usually located
///   at `/etc/resolv.conf`. It can be overridden via config for easier testing.
///
/// - **handoffQueueDepth**
///
///   Size of Memberlist's internal channel which handles UDP messages. The
///   size of this determines the size of the queue which Memberlist will keep
///   while UDP messages are handled.
///
/// - **packetBufferSize**
///
///   Maximum number of bytes that memberlist will put in a packet (this
///   will be for UDP packets by default with a NetTransport). A safe value
///   for this is typically 1400 bytes (which is the default). However,
///   depending on your network's MTU (Maximum Transmission Unit) you may
///   be able to increase this to get more content into each gossip packet.
///
/// - **deadNodeReclainTime**
///
///   Controls the time before a dead node's name can be
///   reclaimed by one with a different address or port. By default, this is 0,
///   meaning nodes cannot be reclaimed this way.
///
/// - **requireNodeNames**
///
///   Controls if the name of a node is required when sending
///   a message to that node.
///
/// - **allowedCIDRs**
///
///   If null, allow any connection (default), otherwise specify all networks
///   allowed to connect (you must specify IPv6/IPv4 separately)
///   Using an empty Vec will block all connections.
///
/// - **quickCheckInterval**
///
///   The interval at which we check the message
///   queue to apply the warning and max depth.
#[napi]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Options {
  /// The name of this node. This must be unique in the cluster.
  name: SmolStr,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  label: Bytes,

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
  secret_key: Option<SecretKey>,

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
  dns_config_path: PathBuf,

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

impl TryFrom<Options> for ROptions {
  type Error = String;

  fn try_from(value: Options) -> std::result::Result<Self, Self::Error> {
    Ok(Self {
      name: value.name,
      label: value.label,
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
      secret_key: value.secret_key,
      delegate_protocol_version: value.delegate_protocol_version,
      delegate_protocol_min: value.delegate_protocol_min,
      delegate_protocol_max: value.delegate_protocol_max,
      dns_config_path: value.dns_config_path,
      handoff_queue_depth: value.handoff_queue_depth,
      packet_buffer_size: value.packet_buffer_size,
      dead_node_reclaim_time: value.dead_node_reclaim_time.into(),
      require_node_names: value.require_node_names,
      allowed_cidrs: value
        .allowed_cidrs
        .map(|x| {
          x.into_iter()
            .map(|x| IpNet::from_str(x.as_str()).map_err(|e| e.to_string()))
            .collect::<std::result::Result<std::collections::HashSet<_>, _>>()
        })
        .transpose()?,
      queue_check_interval: value.queue_check_interval.into(),
    })
  }
}

impl From<ROptions> for Options {
  fn from(value: ROptions) -> Self {
    Self {
      name: value.name,
      label: value.label,
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
      secret_key: value.secret_key,
      delegate_protocol_version: value.delegate_protocol_version,
      delegate_protocol_min: value.delegate_protocol_min,
      delegate_protocol_max: value.delegate_protocol_max,
      dns_config_path: value.dns_config_path,
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

#[napi]
impl Options {
  /// Returns a sane set of configurations for Memberlist.
  /// It uses the hostname as the node name, and otherwise sets very conservative
  /// values that are sane for most LAN environments. The default configuration
  /// errs on the side of caution, choosing values that are optimized
  /// for higher convergence at the cost of higher bandwidth usage. Regardless,
  /// these values are a good starting point when getting started with memberlist.
  #[napi(factory)]
  pub fn lan() -> Self {
    ROptions::lan().into()
  }

  /// Returns the default configurations, which is the same as wan
  #[napi(constructor)]
  pub fn new() -> Self {
    ROptions::wan().into()
  }

  /// Returns a configuration
  /// that is optimized for most WAN environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[napi(factory)]
  pub fn wan() -> Self {
    ROptions::wan().into()
  }

  /// Returns a configuration
  /// that is optimized for a local loopback environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[napi(factory)]
  pub fn local() -> Self {
    ROptions::local().into()
  }

  #[napi(getter)]
  pub fn name(&self) -> &str {
    self.name.as_str()
  }
  #[napi(getter)]
  pub fn label(&self) -> Vec<u8> {
    self.label.to_vec()
  }
  #[napi(getter)]
  pub fn skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
  #[napi(getter)]
  pub fn bind_addr(&self) -> String {
    self.bind_addr.to_string()
  }
  #[napi(getter)]
  pub fn advertise_addr(&self) -> Option<String> {
    self.advertise_addr.as_ref().map(|s| s.to_string())
  }
  #[napi(getter)]
  pub fn protocol_version(&self) -> u8 {
    self.protocol_version
  }
  #[napi(getter)]
  pub fn tcp_timeout(&self) -> Duration {
    self.tcp_timeout.into()
  }
  #[napi(getter)]
  pub fn indirect_checks(&self) -> u32 {
    self.indirect_checks as u32
  }
  #[napi(getter)]
  pub fn retransmit_mult(&self) -> u32 {
    self.retransmit_mult as u32
  }
  #[napi(getter)]
  pub fn suspicion_mult(&self) -> u32 {
    self.suspicion_mult as u32
  }
  #[napi(getter)]
  pub fn suspicion_max_timeout_mult(&self) -> u32 {
    self.suspicion_max_timeout_mult as u32
  }
  #[napi(getter)]
  pub fn push_pull_interval(&self) -> Duration {
    self.push_pull_interval.into()
  }
  #[napi(getter)]
  pub fn probe_interval(&self) -> Duration {
    self.probe_interval.into()
  }
  #[napi(getter)]
  pub fn probe_timeout(&self) -> Duration {
    self.probe_timeout.into()
  }
  #[napi(getter)]
  pub fn disable_tcp_pings(&self) -> bool {
    self.disable_tcp_pings
  }
  #[napi(getter)]
  pub fn awareness_max_multiplier(&self) -> u32 {
    self.awareness_max_multiplier as u32
  }
  #[napi(getter)]
  pub fn gossip_interval(&self) -> Duration {
    self.gossip_interval.into()
  }
  #[napi(getter)]
  pub fn gossip_nodes(&self) -> u32 {
    self.gossip_nodes as u32
  }
  #[napi(getter)]
  pub fn gossip_to_the_dead_time(&self) -> Duration {
    self.gossip_to_the_dead_time.into()
  }
  #[napi(getter)]
  pub fn gossip_verify_incoming(&self) -> bool {
    self.gossip_verify_incoming
  }
  #[napi(getter)]
  pub fn gossip_verify_outgoing(&self) -> bool {
    self.gossip_verify_outgoing
  }
  #[napi(getter)]
  pub fn enable_compression(&self) -> bool {
    self.enable_compression
  }
  #[napi(getter)]
  pub fn secret_key(&self) -> Option<Vec<u8>> {
    self.secret_key.clone().map(|x| x.to_vec())
  }
  #[napi(getter)]
  pub fn delegate_protocol_version(&self) -> u8 {
    self.delegate_protocol_version
  }
  #[napi(getter)]
  pub fn delegate_protocol_min(&self) -> u8 {
    self.delegate_protocol_min
  }
  #[napi(getter)]
  pub fn delegate_protocol_max(&self) -> u8 {
    self.delegate_protocol_max
  }
  #[napi(getter)]
  pub fn dns_config_path(&self) -> String {
    self.dns_config_path.display().to_string()
  }
  #[napi(getter)]
  pub fn handoff_queue_depth(&self) -> u32 {
    self.handoff_queue_depth as u32
  }
  #[napi(getter)]
  pub fn packet_buffer_size(&self) -> u32 {
    self.packet_buffer_size as u32
  }
  #[napi(getter)]
  pub fn dead_node_reclaim_time(&self) -> Duration {
    self.dead_node_reclaim_time.into()
  }
  #[napi(getter)]
  pub fn require_node_names(&self) -> bool {
    self.require_node_names
  }
  #[napi(getter, js_name = "allowedCIDRs")]
  pub fn allowed_cidrs(&self) -> Option<Vec<String>> {
    match &self.allowed_cidrs {
      Some(cidrs) => Some(cidrs.iter().map(|x| x.to_string()).collect()),
      None => None,
    }
  }
  #[napi(getter)]
  pub fn queue_check_interval(&self) -> Duration {
    self.queue_check_interval.into()
  }
  #[napi(writable = false)]
  pub fn set_name(&mut self, val: String) -> &mut Options {
    self.name = val.into();
    self
  }
  #[napi(writable = false)]
  pub fn set_label(&mut self, val: Vec<u8>) -> &mut Options {
    self.label = val.into();
    self
  }
  #[napi(writable = false)]
  pub fn set_skip_inbound_label_check(&mut self, val: bool) -> &mut Options {
    self.skip_inbound_label_check = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_bind_addr(&mut self, val: String) -> Result<&mut Options> {
    self.bind_addr = val
      .parse()
      .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
    Ok(self)
  }

  #[napi(writable = false)]
  pub fn set_advertise_addr(&mut self, val: Option<String>) -> Result<&mut Options> {
    self.advertise_addr = if let Some(val) = val {
      Some(
        val
          .parse()
          .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?,
      )
    } else {
      None
    };
    Ok(self)
  }
  #[napi(writable = false)]
  pub fn set_protocol_version(&mut self, val: u8) -> &mut Options {
    self.protocol_version = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_tcp_timeout(&mut self, val: &Duration) -> &mut Options {
    self.tcp_timeout = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_indirect_checks(&mut self, val: u32) -> &mut Options {
    self.indirect_checks = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_retransmit_mult(&mut self, val: u32) -> &mut Options {
    self.retransmit_mult = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_suspicion_mult(&mut self, val: u32) -> &mut Options {
    self.suspicion_mult = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_suspicion_max_timeout_mult(&mut self, val: u32) -> &mut Options {
    self.suspicion_max_timeout_mult = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_push_pull_interval(&mut self, val: &Duration) -> &mut Options {
    self.push_pull_interval = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_probe_interval(&mut self, val: &Duration) -> &mut Options {
    self.probe_interval = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_probe_timeout(&mut self, val: &Duration) -> &mut Options {
    self.probe_timeout = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_disable_tcp_pings(&mut self, val: bool) -> &mut Options {
    self.disable_tcp_pings = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_awareness_max_multiplier(&mut self, val: u32) -> &mut Options {
    self.awareness_max_multiplier = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_gossip_interval(&mut self, val: &Duration) -> &mut Options {
    self.gossip_interval = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_gossip_nodes(&mut self, val: u32) -> &mut Options {
    self.gossip_nodes = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_gossip_to_the_dead_time(&mut self, val: &Duration) -> &mut Options {
    self.gossip_to_the_dead_time = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_gossip_verify_incoming(&mut self, val: bool) -> &mut Options {
    self.gossip_verify_incoming = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_gossip_verify_outgoing(&mut self, val: bool) -> &mut Options {
    self.gossip_verify_outgoing = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_enable_compression(&mut self, val: bool) -> &mut Options {
    self.enable_compression = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_secret_key(&mut self, val: Option<Vec<u8>>) -> Result<&mut Options> {
    self.secret_key = if let Some(val) = val {
      Some(
        showbiz_core::SecretKey::try_from(val.as_ref())
          .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?,
      )
    } else {
      None
    };
    Ok(self)
  }
  #[napi(writable = false)]
  pub fn set_delegate_protocol_version(&mut self, val: u8) -> &mut Options {
    self.delegate_protocol_version = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_delegate_protocol_min(&mut self, val: u8) -> &mut Options {
    self.delegate_protocol_min = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_delegate_protocol_max(&mut self, val: u8) -> &mut Options {
    self.delegate_protocol_max = val;
    self
  }
  #[napi(writable = false)]
  pub fn set_dns_config_path(&mut self, val: String) -> &mut Options {
    self.dns_config_path = std::path::PathBuf::from(val);
    self
  }
  #[napi(writable = false)]
  pub fn set_handoff_queue_depth(&mut self, val: u32) -> &mut Options {
    self.handoff_queue_depth = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_packet_buffer_size(&mut self, val: u32) -> &mut Options {
    self.packet_buffer_size = val as usize;
    self
  }
  #[napi(writable = false)]
  pub fn set_dead_node_reclaim_time(&mut self, val: &Duration) -> &mut Options {
    self.dead_node_reclaim_time = (*val).into();
    self
  }
  #[napi(writable = false)]
  pub fn set_require_node_names(&mut self, val: bool) -> &mut Options {
    self.require_node_names = val;
    self
  }

  #[napi(writable = false, js_name = "setAllowedCIDRs")]
  pub fn set_allowed_cidrs(&mut self, val: Vec<String>) -> Result<&mut Options> {
    self.allowed_cidrs = if val.is_empty() {
      None
    } else {
      Some(
        val
          .into_iter()
          .map(|x| x.parse())
          .collect::<std::result::Result<Vec<_>, _>>()
          .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?,
      )
    };
    Ok(self)
  }

  #[napi(writable = false)]
  pub fn set_queue_check_interval(&mut self, val: &Duration) -> &mut Options {
    self.queue_check_interval = (*val).into();
    self
  }

  /// Serializes the options to JSON.
  ///
  /// If `pretty` is `true`, the JSON will be pretty-printed.
  #[napi(js_name = "toJSON")]
  pub fn to_json(&self, pretty: Option<bool>) -> Result<String> {
    if let Some(pretty) = pretty {
      if pretty {
        return serde_json::to_string_pretty(self)
          .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")));
      }
    }
    serde_json::to_string(self).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Serializes the options to a JSON file.
  ///
  /// If `pretty` is `true`, the JSON will be pretty-printed.
  #[napi(js_name = "toJSONFile")]
  pub fn to_json_file(&self, path: String, pretty: Option<bool>) -> Result<()> {
    let file = std::fs::File::create(path)
      .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
    if let Some(pretty) = pretty {
      if pretty {
        return serde_json::to_writer_pretty(file, self)
          .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")));
      }
    }
    serde_json::to_writer(file, self)
      .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Serializes the options to YAML.
  #[napi(js_name = "toYAML")]
  pub fn to_yaml(&self) -> Result<String> {
    serde_yaml::to_string(self).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Serializes the options to a YAML file.
  #[napi(js_name = "toYAMLFile")]
  pub fn to_yaml_file(&self, path: String) -> Result<()> {
    let file = std::fs::File::create(path)
      .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
    serde_yaml::to_writer(file, self)
      .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Deserializes the options from JSON string.
  #[napi(factory, js_name = "fromJSON")]
  pub fn from_json(src: String) -> Result<Self> {
    serde_json::from_str(&src).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Deserializes the options from YAML file.
  #[napi(factory, js_name = "fromYAMLFile")]
  pub fn from_json_file(src: String) -> Result<Self> {
    let file =
      std::fs::File::open(&src).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
    serde_yaml::from_reader(file).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Deserializes the options from YAML string.
  #[napi(factory, js_name = "fromYAML")]
  pub fn from_yaml(src: String) -> Result<Self> {
    serde_yaml::from_str(&src).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  /// Deserializes the options from YAML file.
  #[napi(factory, js_name = "fromYAMLFile")]
  pub fn from_yaml_file(src: String) -> Result<Self> {
    let file =
      std::fs::File::open(&src).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))?;
    serde_yaml::from_reader(file).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }
}
