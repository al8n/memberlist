//! Sans-I/O state machine for the [`memberlist`](https://crates.io/crates/memberlist) gossip
//! protocol. Modeled on [`quinn-proto`](https://docs.rs/quinn-proto): a pure state machine
//! that takes events as inputs and emits actions as outputs, never owning I/O or timers.
//!
//! This crate is `Send` but not `Sync` per type. Drivers choose how to share access
//! (mutex, dedicated thread, actor model, etc.).
//!
//! # The driver contract
//!
//! This crate is **only the protocol logic**. Sockets, the clock, TLS,
//! retransmit timers, task scheduling, and event delivery ordering are the
//! *driver's* job. The two responsibilities do not mix: the state machine
//! is correct on its own terms, and it will **not** contain machinery to
//! paper over a driver-layer problem. Read this section before writing a
//! driver.
//!
//! ## The two state machines
//!
//! * [`endpoint::Endpoint`] — one per local node. Owns membership
//!   (alive/suspect/dead/refute/merge), the SWIM probe FSM, Lifeguard
//!   awareness, suspicion timers, and the gossip broadcast queue.
//! * [`stream::Stream`] — one per *reliable exchange* (push/pull, reliable
//!   ping, reliable user message). Minted by
//!   [`Endpoint::dial_succeeded`](endpoint::Endpoint::dial_succeeded)
//!   (outbound) or [`Endpoint::accept_stream`](endpoint::Endpoint::accept_stream)
//!   (inbound).
//!
//! The driver **bridges** them: drain a `Stream`'s
//! [`poll_endpoint_event`](stream::Stream::poll_endpoint_event) and feed
//! each into
//! [`Endpoint::handle_stream_event`](endpoint::Endpoint::handle_stream_event).
//!
//! ## The driver loop
//!
//! Per wake-up the driver, holding whatever lock it chose:
//!
//! 1. feeds inbound datagrams via
//!    [`handle_packet`](endpoint::Endpoint::handle_packet) (or the typed
//!    `handle_*` methods) with the **observed transport source address**
//!    and the current `now`;
//! 2. pumps each live `Stream`: `handle_data` for received bytes (or an
//!    **empty slice for EOF** — see below), drain `poll_transmit(now, ..)`
//!    to the socket, route `poll_endpoint_event` into the `Endpoint`,
//!    surface `poll_event`, and reap the stream once
//!    [`is_done`](stream::Stream::is_done) or
//!    [`is_failed`](stream::Stream::is_failed);
//! 3. fires [`Endpoint::handle_timeout(now)`](endpoint::Endpoint::handle_timeout)
//!    (and each Stream's `handle_timeout`);
//! 4. drains [`poll_transmit`](endpoint::Endpoint::poll_transmit) and
//!    [`poll_event`](endpoint::Endpoint::poll_event) **until `None`**;
//! 5. sleeps until [`poll_timeout`](endpoint::Endpoint::poll_timeout)
//!    (min'd with each Stream's `poll_timeout`), then repeats.
//!
//! ## Time contract
//!
//! Every entry point that can advance state takes `now: Instant`. The
//! driver **MUST** pass a *non-decreasing* `now` across calls. Internal
//! deadlines are anchored absolutely at the instant they are created
//! (e.g. a probe's failure deadline is `sent + interval`, snapshotted
//! once), never recomputed from a later injected `now` — so a late timer
//! callback or packet-vs-timer reordering cannot move them. Over-firing
//! `handle_timeout` (calling it before any deadline elapsed) is always
//! safe and is a no-op.
//!
//! ## Correctness vs. quality (read this)
//!
//! The state machine is **correct under ANY input ordering**. A late or
//! out-of-order event never corrupts state: a deadline is authoritative
//! at the point of action (an ack/transmit/read whose `now >= deadline`
//! is rejected, not retroactively undone), and any residual disagreement
//! converges through the protocol's own self-healing (e.g. a node briefly
//! suspected by a lost race refutes with a higher incarnation). The driver
//! therefore does **not** need to pre-filter stale input for correctness.
//!
//! What *does* depend on the driver is **quality**: failure-detection
//! latency and how often a healthy node is transiently suspected. These
//! improve the more promptly and in-causal-order the driver delivers
//! input. In particular, routing a reliable `Stream`'s endpoint events
//! into the `Endpoint` *before* firing the `Endpoint`'s cumulative-probe
//! `handle_timeout` narrows the transient-suspect window. That is a
//! driver-side optimization, **not** a correctness requirement the
//! machine relies on — the machine never compensates for a driver that
//! delivers late; it just self-heals, and the driver owns making delivery
//! prompt.
//!
//! ## Stream specifics
//!
//! * **EOF is an empty slice.** The driver signals peer-close / clean
//!   shutdown by calling
//!   [`Stream::handle_data(&[], now)`](stream::Stream::handle_data). A
//!   non-terminal stream treats that as `PeerClosed`.
//! * **Draining `poll_transmit` until `None` is the whole write
//!   contract.** Handing over the last byte is what advances the write
//!   phase; there is no separate "write complete" call.
//! * A `Stream` is *minted by* the `Endpoint` but driven *independently*;
//!   it carries the exchange deadline and is the authority on it.

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![allow(clippy::type_complexity, unexpected_cfgs)]
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
pub use quic::{DatagramSendOutcome, QuicConfig, QuicEndpoint, UnreliableTransport};

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
pub use tls::{TlsOptions, TlsRecords};

#[cfg(feature = "tcp")]
mod tcp;
#[cfg(feature = "tcp")]
pub use tcp::RawRecords;

// The cluster-label record-layer decorator and its options bundle are shared
// by every reliable transport (plain TCP via `Labeled<Passthrough>`, TLS via
// `Labeled<TlsRecords>`), so they are re-exported whenever any stream
// transport is built. `LabelOptionsError` rides alongside for the fallible
// label-validating constructors.
#[cfg(any(feature = "tls", feature = "tcp"))]
pub use streams::{LabelOptions, LabelOptionsError, Labeled, Passthrough};

#[cfg(any(feature = "tls", feature = "tcp"))]
pub mod streams;

pub mod ack;
pub mod awareness;
pub mod broadcast;
pub mod config;
pub mod delegate;
pub mod endpoint;
pub mod error;
pub mod event;
mod mathf;
pub mod members;
pub(crate) mod probe;
pub mod stream;
pub mod suspicion;
pub mod time;
pub(crate) mod wire;

// ── Wire types, codec, and framing ──────────────────────────────────────────
pub mod bridge;
pub mod codec;
pub mod compression;
pub mod convert;
pub mod data;
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
pub use config::{DEFAULT_GOSSIP_MTU, EndpointConfig};
pub use delegate::{AliveDelegate, MergeDelegate};
pub use endpoint::{Endpoint, Lifecycle, META_MAX_SIZE};
pub use error::{EndpointInitError, Error, StreamError};
pub use event::{
  CompoundTransmit, DecodeError, DialRequested, EndpointEvent, Event, NodeConflict, PacketTransmit,
  PingCompleted, PingFailed, PingId, PushPullKind, PushPullReplyReceived, PushPullRequestReceived,
  Reliability, ReliablePingAcked, ReliablePingFailed, RemoteStateReceived, SendPushPullResponse,
  StreamClosed, StreamCommand, StreamErrored, StreamEvent, StreamId, Transmit, UserDataReceived,
  UserPacket,
};
pub use members::{LocalNodeState, Member, Members};
pub use stream::{PushPullSnapshot, Stream};
pub use suspicion::{Confirmation, Suspicion};
pub use time::Instant;

pub use bridge::{BridgeError, message_from_any, message_to_any};
pub use cheap_clone::CheapClone;
pub use codec::{
  CodecError, DecodeOptions, EncodeOptions, decode_incoming, encode_outgoing,
  encode_outgoing_compound, parse_message, parse_messages,
};
pub use compression::{
  CompressAlgorithm, CompressionError, CompressionOptions, CompressionOutcome, OversizeOriginal,
  UnitLenExceedsMaxInfo, compress, decode_compressed_frame, decompress, encode_compressed_frame,
  encode_reliable_unit, encode_reliable_unit_with_encryption, take_reliable_unit,
  take_reliable_unit_with_encryption,
};
pub use convert::{
  AddrLengthMismatchInfo, ConvertError, id_from_bytes, id_to_bytes, socket_addr_from_bytes,
  socket_addr_to_bytes,
};
pub use label::{
  LABEL_OVERHEAD, LABELED_TAG, LabelError, LabelOutcome, MAX_LABEL_LEN, classify_header,
  effective_label, encode_label_prefix, validate_label,
};
// `data::DecodeError` is reached via `crate::data::DecodeError`; the crate-root
// `DecodeError` name is already taken by `event::DecodeError`.
pub use data::{
  Data, DataRef, DuplicateFieldInfo, EncodeError, InsufficientBufferCapacity, MissingFieldInfo,
  UnknownTagInfo, UnknownWireTypeInfo,
};
pub use encryption::{
  ENCRYPTED_TAG, ENCRYPTED_WRAPPER_OVERHEAD, EncryptAlgorithm, EncryptionError, EncryptionOptions,
  Keyring, KeyringError, OversizeCiphertext, SecretKey, decode_encrypted_frame, decrypt,
  encode_encrypted_frame, encrypt,
};
pub use framing::{
  AnyMessage, COMPOUND_MAX_COUNT_PREFIX_LEN, COMPOUND_MAX_PART_PREFIX_LEN, COMPOUND_TAG_LEN,
  FrameError, IncompleteFrame, MessageTag, decode_compound, decode_message, decode_plain_frame,
  encode_compound, encode_message, encode_plain_frame, unwrap_transforms,
  unwrap_transforms_with_encryption,
};
pub use id::Id;
pub use node::Node;

/// `FxHashMap`/`FxHashSet` backed by hashbrown (no_std-capable) with rustc-hash's
/// Fx hasher — rustc-hash's own `Fx*` map aliases are std-only.
pub(crate) type FxHashMap<K, V> = hashbrown::HashMap<K, V, rustc_hash::FxBuildHasher>;
pub(crate) type FxHashSet<T> = hashbrown::HashSet<T, rustc_hash::FxBuildHasher>;
