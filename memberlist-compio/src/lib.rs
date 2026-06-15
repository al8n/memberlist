//! Completion-based async driver for the Sans-I/O memberlist machine,
//! powered by compio.
#![cfg_attr(docsrs, feature(doc_cfg))]

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

use rand::{
  SeedableRng,
  rngs::{StdRng, SysRng},
};

/// The driver's machine endpoints stay generic over the gossip RNG `G`, exactly
/// like proto, so a caller can inject their own via the `*_with_rng`
/// constructors. `G` defaults to [`StdRng`] (a ChaCha CSPRNG) — the RNG the
/// plain constructors seed from the OS — so the common shorter spellings stay
/// terse and secure by default.
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
pub(crate) type StreamEndpoint<I, A, R, G = StdRng> =
  memberlist_proto::streams::StreamEndpoint<I, A, R, G>;
#[cfg(feature = "quic")]
pub(crate) type QuicEndpoint<I, G = StdRng> = memberlist_proto::QuicEndpoint<I, G>;

/// A fresh `StdRng` seeded directly from the operating system entropy source
/// ([`SysRng`], i.e. `getrandom`) — never from a thread-local generator, so a
/// process that forks after building a node cannot inherit a parent's RNG state
/// and derive the same gossip schedule. This is the default RNG the plain
/// constructors seed; `*_with_rng` callers supply their own instead.
///
/// Drawn in `Memberlist::new` (which returns a `Result`) BEFORE the driver task
/// is spawned, then passed into `Transport::run` and used to build the machine
/// `Endpoint`. An OS entropy failure is therefore surfaced as
/// [`MemberlistError::Entropy`] rather than panicking in the spawned task after
/// the handle was already returned.
pub(crate) fn gossip_rng() -> crate::Result<StdRng> {
  StdRng::try_from_rng(&mut SysRng)
    .map_err(|e| crate::MemberlistError::Entropy(std::io::Error::other(e)))
}

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
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use memberlist_proto::{AddrParseError, CidrPolicy, IpNet};
pub use memberlist_proto::{
  ChecksumAlgorithm, ChecksumOptions, Node, typed::NodeState as MemberlistNodeState,
};
pub use options::{MemberlistOptions, Options, OptionsParts};
pub use resolver::{
  AdvertiseAddrResolver, AdvertiseResolutionError, FirstAddrResolver, Ipv4PreferringResolver,
  Ipv6PreferringResolver, OsResolver, Resolver, SocketAddrResolver,
};
pub use snapshot::MemberlistSnapshot;
pub use transport::{Transport, TransportRuntime};

#[cfg(feature = "tcp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
pub use tcp::{TcpTransport, TcpTransportOptions};

#[cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs")))
)]
pub use tls::{SniProvider, TlsTransport, TlsTransportOptions};

#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use quic::{Quic, QuicOptions, QuicTransport, QuicTransportOptions};

#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub use resolver::{DEFAULT_DNS_TIMEOUT, DnsResolver};
#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
pub use resolver::{LocalAddrResolver, LocalAddrScope, local_advertise};
