use std::{
  collections::HashSet,
  net::{IpAddr, Ipv4Addr},
  path::PathBuf,
  str::FromStr,
  sync::Arc,
  time::Duration,
};

use crate::security::SecretKeyring;

use super::{
  security::{EncryptionAlgo, SecretKey},
  transport::{Transport, TransportOptions},
  types::{CompressionAlgo, Label, Name},
  version::VSN_SIZE,
};

pub use super::version::{DelegateVersion, ProtocolVersion};

const DEFAULT_PORT: u16 = 7946;

#[viewit::viewit(getters(vis_all = "pub"), setters(vis_all = "pub", prefix = "with"))]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Options<T: Transport> {
  /// The name of this node. This must be unique in the cluster.
  #[viewit(getter(const, style = "ref"))]
  name: Name,

  /// Label is an optional set of bytes to include on the outside of each
  /// packet and stream.
  ///
  /// If gossip encryption is enabled and this is set it is treated as GCM
  /// authenticated data.
  #[viewit(getter(const, style = "ref"))]
  label: Label,

  /// Skips the check that inbound packets and gossip
  /// streams need to be label prefixed.
  skip_inbound_label_check: bool,

  /// Configuration related to what address to bind to
  /// listen on. The port is used for both UDP and TCP gossip.
  bind_addr: IpAddr,
  /// Configuration related to what port to bind to
  /// listen on. The port is used for both UDP and TCP gossip. It is
  /// assumed other nodes are running on this port, but they do not need
  /// to.
  bind_port: Option<u16>,

  /// Configuration related to what address to advertise to other
  /// cluster members. Used for nat traversal.
  advertise_addr: Option<IpAddr>,
  advertise_port: Option<u16>,

  /// The configured encryption type that we
  /// will _speak_. This must be between [`EncryptionAlgo::MIN`] and
  /// [`EncryptionAlgo::MAX`].
  encryption_algo: EncryptionAlgo,

  /// The timeout for establishing a stream connection with
  /// a remote node for a full state sync, and for stream read and write
  /// operations. This is a legacy name for backwards compatibility, but
  /// should really be called StreamTimeout now that we have generalized
  /// the transport.
  #[serde(with = "humantime_serde")]
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
  #[serde(with = "humantime_serde")]
  push_pull_interval: Duration,

  /// The interval between random node probes. Setting
  /// this lower (more frequent) will cause the memberlist cluster to detect
  /// failed nodes more quickly at the expense of increased bandwidth usage
  #[serde(with = "humantime_serde")]
  probe_interval: Duration,
  /// The timeout to wait for an ack from a probed node
  /// before assuming it is unhealthy. This should be set to 99-percentile
  /// of RTT (round-trip time) on your network.
  #[serde(with = "humantime_serde")]
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
  #[serde(with = "humantime_serde")]
  gossip_interval: Duration,
  /// The number of random nodes to send gossip messages to
  /// per `gossip_interval`. Increasing this number causes the gossip messages
  /// to propagate across the cluster more quickly at the expense of
  /// increased bandwidth.
  gossip_nodes: usize,
  /// The interval after which a node has died that
  /// we will still try to gossip to it. This gives it a chance to refute.
  #[serde(with = "humantime_serde")]
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
  compression_algo: CompressionAlgo,

  /// The size of a message that should be offload to [`rayon`] thread pool
  /// for encryption or compression.
  ///
  /// The default value is 1KB, which means that any message larger than 1KB
  /// will be offloaded to [`rayon`] thread pool for encryption or compression.
  offload_size: usize,

  /// Used to initialize the primary encryption key in a keyring.
  /// The primary encryption key is the only key used to encrypt messages and
  /// the first key used while attempting to decrypt messages. Providing a
  /// value for this primary key will enable message-level encryption and
  /// verification, and automatically install the key onto the keyring.
  /// The value should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  secret_key: Option<SecretKey>,

  #[viewit(getter(
    style = "ref",
    result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeyring>")
  ))]
  secret_keyring: Option<SecretKeyring>,

  /// Used to guarantee protocol-compatibility
  protocol_version: ProtocolVersion,

  // /// Holds all of the encryption keys used internally. It is
  // /// automatically initialized using the SecretKey and SecretKeys values.
  // #[viewit(getter(style = "ref", result(converter(fn = "Option::as_ref"), type = "Option<&SecretKeyring>")))]
  // secret_keyring: Option<SecretKeyring>,
  /// Used to guarantee protocol-compatibility
  /// for any custom messages that the delegate might do (broadcasts,
  /// local/remote state, etc.). If you don't set these, then the protocol
  /// versions will just be zero, and version compliance won't be done.
  delegate_version: DelegateVersion,

  /// Points to the system's Dns config file, usually located
  /// at `/etc/resolv.conf`. It can be overridden via config for easier testing.
  #[viewit(getter(const, style = "ref"))]
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
  #[serde(with = "humantime_serde")]
  dead_node_reclaim_time: Duration,

  /// If [`None`], allow any connection (default), otherwise specify all networks
  /// allowed to connect (you must specify IPv6/IPv4 separately)
  /// Using an empty Vec will block all connections.
  #[viewit(getter(
    style = "ref",
    result(
      converter(fn = "Option::as_ref"),
      type = "Option<&HashSet<ipnet::IpNet>>"
    )
  ))]
  allowed_cidrs: Option<HashSet<ipnet::IpNet>>,

  /// Transport options
  #[viewit(getter(
    style = "ref",
    const,
    result(converter(fn = "Option::as_ref"), type = "Option<&T::Options>")
  ))]
  #[serde(bound = "T::Options: TransportOptions")]
  transport: Option<T::Options>,

  /// The interval at which we check the message
  /// queue to apply the warning and max depth.
  #[serde(with = "humantime_serde")]
  queue_check_interval: Duration,

  #[viewit(getter(style = "ref", const, attrs(cfg(feature = "metrics"))))]
  #[cfg(feature = "metrics")]
  #[serde(with = "crate::util::label_serde")]
  metric_labels: Arc<Vec<metrics::Label>>,
}

impl<T> Default for Options<T>
where
  T: Transport,
{
  #[inline]
  fn default() -> Self {
    Self::lan()
  }
}

impl<T: Transport> Clone for Options<T> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      transport: self.transport.clone(),
      name: self.name.clone(),
      label: self.label.clone(),
      dns_config_path: self.dns_config_path.clone(),
      allowed_cidrs: self.allowed_cidrs.clone(),
      secret_keyring: self.secret_keyring.clone(),
      #[cfg(feature = "metrics")]
      metric_labels: self.metric_labels.clone(),
      ..*self
    }
  }
}

impl<T: Transport> Options<T> {
  #[inline]
  pub const fn build_vsn_array(&self) -> [u8; VSN_SIZE] {
    [self.protocol_version as u8, self.delegate_version as u8]
  }

  /// Returns a sane set of configurations for Memberlist.
  /// It uses the hostname as the node name, and otherwise sets very conservative
  /// values that are sane for most LAN environments. The default configuration
  /// errs on the side of caution, choosing values that are optimized
  /// for higher convergence at the cost of higher bandwidth usage. Regardless,
  /// these values are a good starting point when getting started with memberlist.
  #[inline]
  pub fn lan() -> Self {
    #[cfg(not(any(target_arch = "wasm32", windows)))]
    let hostname = {
      let uname = rustix::system::uname();
      Name::from_string(uname.nodename().to_string_lossy().to_string()).unwrap_or_default()
    };

    #[cfg(windows)]
    let hostname = {
      match hostname::get() {
        Ok(name) => Name::from_string(name.to_string_lossy().to_string()).unwrap_or_default(),
        Err(_) => Name::default(),
      }
    };

    #[cfg(target_family = "wasm")]
    let hostname = Name::default();

    Self {
      name: hostname,
      label: Label::empty(),
      skip_inbound_label_check: false,
      bind_addr: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
      bind_port: Some(DEFAULT_PORT),
      advertise_addr: None,
      advertise_port: Some(DEFAULT_PORT),
      encryption_algo: EncryptionAlgo::MAX,
      tcp_timeout: Duration::from_secs(10), // Timeout after 10 seconds
      indirect_checks: 3,                   // Use 3 nodes for the indirect ping
      retransmit_mult: 4,                   // Retransmit a message 4 * log(N+1) nodes
      suspicion_mult: 4,                    // Suspect a node for 4 * log(N+1) * Interval
      suspicion_max_timeout_mult: 6, // For 10k nodes this will give a max timeout of 120 seconds
      push_pull_interval: Duration::from_secs(30), // Low frequency
      probe_interval: Duration::from_millis(500), // Failure check every second
      probe_timeout: Duration::from_secs(1), // Reasonable RTT time for LAN
      disable_tcp_pings: false,      // TCP pings are safe, even with mixed versions
      awareness_max_multiplier: 8,   // Probe interval backs off to 8 seconds
      gossip_interval: Duration::from_millis(200), // Gossip every 200ms
      gossip_nodes: 3,               // Gossip to 3 nodes
      gossip_to_the_dead_time: Duration::from_secs(30), // same as push/pull
      gossip_verify_incoming: true,
      gossip_verify_outgoing: true,
      compression_algo: CompressionAlgo::Lzw, // Enable compression by default
      secret_key: None,
      secret_keyring: None,
      delegate_version: DelegateVersion::V0,
      protocol_version: ProtocolVersion::V0,
      dns_config_path: PathBuf::from("/etc/resolv.conf"),
      handoff_queue_depth: 1024,
      offload_size: 1024, // 1KB
      packet_buffer_size: 1400,
      dead_node_reclaim_time: Duration::ZERO,
      allowed_cidrs: None,
      transport: None,
      queue_check_interval: Duration::from_secs(30),
      #[cfg(feature = "metrics")]
      metric_labels: Arc::new(Vec::new()),
    }
  }

  /// Returns a configuration
  /// that is optimized for most WAN environments. The default configuration is
  /// still very conservative and errs on the side of caution.
  #[inline]
  pub fn wan() -> Self {
    Self::lan()
      .with_tcp_timeout(Duration::from_secs(30))
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
      .with_tcp_timeout(Duration::from_secs(1))
      .with_indirect_checks(1)
      .with_retransmit_mult(2)
      .with_suspicion_mult(3)
      .with_push_pull_interval(Duration::from_secs(15))
      .with_probe_timeout(Duration::from_millis(200))
      .with_probe_interval(Duration::from_secs(1))
      .with_gossip_interval(Duration::from_millis(100))
      .with_gossip_to_the_dead_time(Duration::from_secs(15))
  }

  /// Return true if `allowed_cidrs` must be called
  #[inline]
  pub fn ip_must_be_checked(&self) -> bool {
    self
      .allowed_cidrs
      .as_ref()
      .map(|x| !x.is_empty())
      .unwrap_or(false)
  }

  #[inline]
  pub fn ip_allowed(&self, addr: IpAddr) -> Result<(), ForbiddenIp> {
    if !self.ip_must_be_checked() {
      return Ok(());
    }

    for n in self.allowed_cidrs.as_ref().unwrap().iter() {
      let ip = ipnet::IpNet::from(addr);
      if n.contains(&ip) {
        return Ok(());
      }
    }

    Err(ForbiddenIp(addr))
  }
}

// ParseCIDRs return a possible empty list of all Network that have been parsed
// In case of error, it returns succesfully parsed CIDRs and the last error found
pub fn parse_cidrs(v: &[impl AsRef<str>]) -> (HashSet<ipnet::IpNet>, Option<Vec<String>>) {
  let mut nets = HashSet::new();
  let mut errs = Vec::new();

  for p in v.iter() {
    let p = p.as_ref();
    match ipnet::IpNet::from_str(p.trim()) {
      Ok(net) => {
        nets.insert(net);
      }
      Err(_) => {
        errs.push(format!("invalid cidr: {}", p));
      }
    }
  }
  (nets, (!errs.is_empty()).then_some(errs))
}

#[derive(Debug, Clone, Copy)]
pub struct ForbiddenIp(IpAddr);

impl core::fmt::Display for ForbiddenIp {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "IP {} is not allowed", self.0)
  }
}

impl std::error::Error for ForbiddenIp {}
