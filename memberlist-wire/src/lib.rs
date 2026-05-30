//! Wire types and framing for the memberlist gossip protocol.
//!
//! This crate is the buffa-generated successor to `memberlist-proto`.
//! `memberlist-proto` itself stays frozen as a backup; new code should
//! depend on `memberlist-wire` instead. The structure mirrors how
//! `memberlist-core` was preserved while `memberlist-machine` took over.
//!
//! # Modules
//!
//! - [`messages`] — buffa-generated owned + view types for the 10 inner
//!   wire messages (Alive, Suspect, Dead, Ping, IndirectPing, Ack, Nack,
//!   PushPull, UserData, ErrorResponse) plus supporting structures
//!   (Server, PushNodeState, Meta, State enum, version enums).
//! - [`compression`] — the tagged, feature-gated compression transform
//!   (`CompressAlgorithm`, `CompressionOptions`, the compressed-frame
//!   codec). Wraps the [`framing`] codec; opt-in per backend.
//! - [`encryption`] — the tagged, feature-gated AEAD encryption transform
//!   (AES-GCM / ChaCha20-Poly1305). Nests outside compression in the codec
//!   stack; opt-in per backend via `aes-gcm` / `chacha20-poly1305` features.
//! - [`framing`] — hand-rolled outer framing (label, encryption,
//!   checksum, compression, compound). Not protobuf-shaped. Ported from
//!   `memberlist-proto::proto::{encoder, decoder}`.
//! - [`convert`] — boundary helpers translating between buffa-generated
//!   types (raw `bytes` fields) and application types (`SmolStr`,
//!   `SocketAddr`, etc.).
//!
//! # Why a new crate
//!
//! The existing `memberlist-proto` codec was hand-written. Migrating to
//! buffa-generated code gives:
//!
//! - A `.proto` schema that end users can target with `protoc` / `buf`
//!   for cross-language interop.
//! - Codegen consistency — no more chance of off-by-one mistakes in
//!   manual varint or length-prefix logic.
//! - buffa's two-tier owned/view types with `OwnedView<V>` for parking
//!   decoded messages across `await` points without field-by-field clones.
//!
//! `memberlist-proto` is preserved unchanged for backwards compatibility
//! and as a reference implementation that the framing port cross-decodes
//! against to guarantee bit-identical wire output.

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

// Alias `alloc` to the name `std` so genuine-heap `std::` paths (`std::vec`,
// `std::collections::BTreeMap`, `std::sync::Arc`, …) compile unchanged under
// no_std+alloc. Core-resident items are imported from `core::` directly, never
// routed through this alias.
#[cfg(all(not(feature = "std"), feature = "alloc"))]
#[macro_use]
extern crate alloc as std;

#[cfg(feature = "std")]
extern crate std;

// The typed messages, framing, and codecs are intrinsically heap-backed
// (Vec/String/maps), so a build with neither capability tier is unsupported.
// Fail with a clear message instead of a cascade of unresolved-`std` errors.
#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!(
  "memberlist-wire requires the `std` or `alloc` feature (the codec needs a heap allocator)"
);

pub mod bridge;
pub mod compression;
pub mod convert;
pub mod data;
pub mod encryption;
pub mod framing;
pub mod id;
pub mod messages;
pub mod node;
pub mod typed;
pub mod wire_type;

pub use bridge::{BridgeError, message_from_any, message_to_any};
pub use cheap_clone::CheapClone;
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
pub use data::{
  Data, DataRef, DecodeError, DuplicateFieldInfo, EncodeError, InsufficientBufferCapacity,
  MissingFieldInfo, UnknownTagInfo, UnknownWireTypeInfo,
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
