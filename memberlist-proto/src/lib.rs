#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
// `collapsible_if`: the nested `if cond { if let ... }` form is kept deliberately —
// flattening multi-level guards into one long let-chain reads worse here.
#![allow(clippy::collapsible_if, clippy::type_complexity, unexpected_cfgs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

// Alias `alloc` to the name `std` so genuine-heap `std::` paths compile unchanged
// under no_std+alloc (and `#[macro_use]` brings `vec!`/`format!` crate-wide).
// Core-resident items are imported from `core::` directly, never via this alias.
#[cfg(all(not(feature = "std"), feature = "alloc"))]
#[macro_use]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

// The protocol state is intrinsically heap-backed (Vec/Box/String/maps), so a
// build with neither capability tier is unsupported. Fail with a clear message
// instead of a cascade of "cannot find type `Vec`" errors.
#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!(
  "memberlist-proto requires the `std` or `alloc` feature (the protocol state needs a heap allocator)"
);

#[cfg(any(feature = "quic", feature = "tls", feature = "tcp"))]
mod bridge_phase;

#[cfg(feature = "quic")]
mod quic;
#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use quic::{DatagramSendStatus, QuicEndpoint, QuicOptions, UnreliableTransport};
#[cfg(all(feature = "quic", feature = "tls"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "quic", feature = "tls"))))]
pub use quic::{QuicConfigError, QuicConfigOptions};

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use streams::TlsEndpoint;
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use tls::{
  ClientAuthMode, ParseClientAuthModeError, TlsConfigError, TlsConfigOptions, TlsOptions,
  TlsRecords,
};

#[cfg(feature = "tcp")]
mod tcp;
#[cfg(feature = "tcp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
pub use streams::TcpEndpoint;
#[cfg(feature = "tcp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
pub use tcp::RawRecords;

// The cluster-label record-layer decorator and its options bundle are shared
// by every reliable transport (plain TCP via `Labeled<Passthrough>`, TLS via
// `Labeled<TlsRecords>`), so they are re-exported whenever any stream
// transport is built. `LabelOptionsError` rides alongside for the fallible
// label-validating constructors.
#[cfg(any(feature = "tls", feature = "tcp"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "tls", feature = "tcp"))))]
pub use streams::{LabelOptions, LabelOptionsError, Labeled, Passthrough};

#[cfg(any(feature = "tls", feature = "tcp"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "tls", feature = "tcp"))))]
pub mod streams;

pub mod ack;
pub mod awareness;
pub mod broadcast;
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub mod cidr;
pub mod config;
pub mod delegate;
pub mod endpoint;
pub mod error;
pub mod event;
mod mathf;
pub mod maybe_owned;
pub mod maybe_resolved;
pub mod members;
pub mod metrics;
pub(crate) mod probe;
pub mod stream;
pub mod suspicion;
pub mod time;
pub(crate) mod wire;

// ── Wire types, codec, and framing ──────────────────────────────────────────
pub mod bridge;
#[cfg(checksum)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3"
  )))
)]
pub mod checksum;
pub mod codec;
#[cfg(compression)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(
    feature = "lz4",
    feature = "snappy",
    feature = "zstd",
    feature = "brotli"
  )))
)]
pub mod compression;
pub mod convert;
pub mod data;
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20poly1305")))
)]
pub mod encryption;
pub mod framing;
pub mod id;
pub mod label;
pub mod messages;
pub mod node;
pub mod typed;
pub mod wire_type;

pub use ack::{AckEntry, AckKind, AckRegistry, AckResolution, ForwardAck};
pub use awareness::Awareness;
pub use broadcast::{Broadcast, BroadcastQueue, MemberlistBroadcast};
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use cidr::{CidrAnd, CidrPolicy};
pub use config::{DEFAULT_GOSSIP_MTU, EndpointOptions};
pub use delegate::{AliveDelegate, MergeDelegate};
pub use endpoint::{Endpoint, Lifecycle, META_MAX_SIZE};
pub use error::{EndpointInitError, Error, SizeExceeded, StreamError};
// The RNG vocabulary backing the `Endpoint<I, A, R>` type parameter (defaulting
// to `SmallRng`), re-exported so a driver can name the trait bound, seed a
// `SmallRng`, and inject it without taking its own direct `rand` dependency.
pub use event::{
  CompoundTransmit, DecodeError, DialRequested, EndpointEvent, Event, NodeConflict, PacketTransmit,
  PingCompleted, PingFailed, PingId, PushPullKind, PushPullReplyReceived, PushPullRequestReceived,
  Reliability, ReliablePingAcked, ReliablePingFailed, RemoteStateReceived, SendPushPullResponse,
  StreamClosed, StreamCommand, StreamErrored, StreamEvent, StreamId, Transmit, UserDataReceived,
  UserPacket,
};
#[cfg(feature = "cidr")]
#[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
pub use ipnet::{AddrParseError, IpNet};
pub use maybe_owned::MaybeOwned;
pub use maybe_resolved::MaybeResolved;
pub use members::{LocalNodeState, Member, Members};
pub use rand::{Rng, SeedableRng, rngs::SmallRng};
pub use stream::{PushPullSnapshot, Stream};
pub use suspicion::{Confirmation, Suspicion};
pub use time::Instant;

pub use bridge::{BridgeError, message_from_any, message_to_any};
pub use cheap_clone::CheapClone;
#[cfg(checksum)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(
    feature = "crc32",
    feature = "xxhash32",
    feature = "xxhash64",
    feature = "xxhash3",
    feature = "murmur3"
  )))
)]
pub use checksum::{
  CHECKSUMED_WRAPPER_OVERHEAD, ChecksumAlgorithm, ChecksumDigest, ChecksumError, ChecksumOptions,
  ChecksumOutput, decode_checksummed_frame, digest, encode_checksummed_frame, verify,
};
pub use codec::{
  CodecError, DecodeOptions, EncodeOptions, TrailingData, TruncatedInput, decode_incoming,
  encode_outgoing, encode_outgoing_compound, parse_message, parse_messages,
};
#[cfg(compression)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(
    feature = "lz4",
    feature = "snappy",
    feature = "zstd",
    feature = "brotli"
  )))
)]
pub use compression::{
  CompressAlgorithm, CompressionError, CompressionOptions, CompressionOutput, OversizeOriginal,
  UnitLenExceedsMaxInfo, compress, decode_compressed_frame, decompress, encode_compressed_frame,
  encode_reliable_unit, take_reliable_unit,
};
// The encryption-aware reliable-unit codec helpers live in the `compression`
// module (present only under a compression backend) AND name `EncryptionOptions`
// (present only under an encryption backend), so they exist only when BOTH a
// compression and an encryption backend are built in.
#[cfg(all(compression, encryption))]
#[cfg_attr(
  docsrs,
  doc(cfg(all(
    any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    ),
    any(feature = "aes-gcm", feature = "chacha20-poly1305")
  )))
)]
pub use compression::{encode_reliable_unit_with_encryption, take_reliable_unit_with_encryption};
pub use convert::{
  AddrLengthMismatchInfo, ConvertError, id_from_bytes, id_to_bytes, socket_addr_from_bytes,
  socket_addr_to_bytes,
};
pub use label::{
  LABEL_OVERHEAD, LABELED_TAG, LabelError, LabelVerdict, MAX_LABEL_LEN, classify_header,
  effective_label, encode_label_prefix, validate_label,
};
// `data::DecodeError` is reached via `crate::data::DecodeError`; the crate-root
// `DecodeError` name is already taken by `event::DecodeError`.
pub use data::{
  Data, DataRef, DuplicateFieldInfo, EncodeError, InsufficientBufferCapacity, MissingFieldInfo,
  UnknownTagInfo, UnknownWireTypeInfo,
};
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use encryption::{
  ENCRYPTED_TAG, ENCRYPTED_WRAPPER_OVERHEAD, EncryptAlgorithm, EncryptionError, EncryptionOptions,
  Keyring, KeyringError, OversizeCiphertext, SecretKey, decode_encrypted_frame, decrypt,
  encode_encrypted_frame, encrypt,
};
pub use framing::{
  AnyMessage, COMPOUND_MAX_COUNT_PREFIX_LEN, COMPOUND_MAX_PART_PREFIX_LEN, COMPOUND_TAG_LEN,
  FrameError, IncompleteFrame, MessageTag, decode_compound, decode_message, decode_plain_frame,
  encode_compound, encode_message, encode_plain_frame, unwrap_transforms,
};
// `unwrap_transforms_with_encryption` strips the outer `Encrypted` wrapper via
// the configured keyring, so it exists only when an encryption backend is built
// in (the base `unwrap_transforms` is always present above).
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
pub use framing::unwrap_transforms_with_encryption;
pub use id::Id;
pub use node::Node;

/// `FxHashMap`/`FxHashSet` backed by hashbrown (no_std-capable) with rustc-hash's
/// Fx hasher — rustc-hash's own `Fx*` map aliases are std-only.
pub(crate) type FxHashMap<K, V> = hashbrown::HashMap<K, V, rustc_hash::FxBuildHasher>;
pub(crate) type FxHashSet<T> = hashbrown::HashSet<T, rustc_hash::FxBuildHasher>;
