//! Completion-based async driver for the Sans-I/O memberlist machine,
//! powered by compio.

mod address;
mod command;
mod delegate;
mod driver_options;
mod driver_shared;
mod error;
mod events;
mod maybe_resolved;
mod memberlist;
mod options;
mod resolver;
mod snapshot;
mod transport;

// The reliable byte-stream plane (`StreamEndpoint` + the per-bridge byte-mover)
// is compiled only for the stream transports. QUIC has its own reliable plane
// (`QuicEndpoint`), so a QUIC-only build does not pull these in.
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
mod bridge;
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
mod driver;

#[cfg(feature = "tcp")]
mod tcp;

#[cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]
mod tls;

#[cfg(feature = "quic")]
mod quic;

#[cfg(feature = "quic")]
mod quic_driver;

pub use address::Address;
pub use delegate::{
  AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, NodeDelegate,
  PingDelegate, VoidDelegate,
};
pub use driver_options::{
  Channel, DEFAULT_BRIDGE_INBOUND_CAP, DEFAULT_BRIDGE_RECV_BUF_LEN, DEFAULT_CMD_FAIRNESS_BUDGET,
  DEFAULT_DIAL_TIMEOUT, DEFAULT_EVENT_QUEUE_CAP, DEFAULT_IDLE_WAKE_INTERVAL,
  DEFAULT_ITER_DRAIN_CAP, DEFAULT_JOIN_DEADLINE, DEFAULT_LEAVE_TIMEOUT,
  DEFAULT_OBSERVATION_CHANNEL, DEFAULT_PEEK_BUDGET, DriverOptions, StreamTransportOptions,
};
pub use error::{
  GossipMtuTooSmall, InvalidAdvertiseAddr, InvalidGossipMtu, InvalidOption, JoinAllFailed,
  MemberlistError, Result,
};
pub use events::EventStream;
pub use maybe_resolved::MaybeResolved;
pub use memberlist::Memberlist;
pub use memberlist_proto::{Node, typed::NodeState as MemberlistNodeState};
pub use options::{MemberlistOptions, Options, OptionsParts};
pub use resolver::{
  AdvertiseAddrResolver, AdvertiseResolutionError, FirstAddrResolver, Ipv4PreferringResolver,
  Ipv6PreferringResolver, OsResolver, Resolver, SocketAddrResolver,
};
pub use snapshot::MemberlistSnapshot;
pub use transport::{Transport, TransportRuntime};

#[cfg(feature = "tcp")]
pub use tcp::{TcpMemberlist, TcpTransport, TcpTransportOptions};

#[cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]
pub use tls::{SniProvider, TlsMemberlist, TlsTransport, TlsTransportOptions};

#[cfg(feature = "quic")]
pub use quic::{Quic, QuicMemberlist, QuicOptions, QuicTransport, QuicTransportOptions};

#[cfg(feature = "dns")]
pub use resolver::{DEFAULT_DNS_TIMEOUT, DnsResolver};
