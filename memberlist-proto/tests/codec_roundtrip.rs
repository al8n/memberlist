//! Cross-transform roundtrip matrix for the wire codec.
//!
//! Drives real typed [`Message<I, A>`](memberlist_proto::typed::Message) values
//! through the codec's outer transform layers — label, compression, checksum,
//! encryption, and their pairwise + all-combined compositions — and asserts the
//! codec's own roundtrip identity: `decode(encode(msgs)) == msgs`, and that the
//! transform was actually applied (a non-trivial wrapper tag is present on the
//! wire).
//!
//! The codec exposes two transform planes, both composed from the same public
//! free functions:
//!
//! * the **gossip/datagram plane** — [`encode_outgoing`] /
//!   [`encode_outgoing_compound`] add the optional label; a `Compressed`
//!   wrapper ([`encode_compressed_frame`]), a `Checksumed` wrapper
//!   ([`encode_checksummed_frame`]), and an `Encrypted` wrapper
//!   ([`encode_encrypted_frame`]) nest inside it (inner→outer: compress →
//!   checksum → encrypt), stripped on the way in by [`decode_incoming`] then
//!   [`unwrap_transforms_with_encryption`] then [`parse_messages`];
//! * the **reliable-stream plane** — [`encode_reliable_unit_with_encryption`] /
//!   [`take_reliable_unit_with_encryption`] fold compression, encryption, and a
//!   self-delimiting length prefix into one unit. It carries no checksum:
//!   stream transports provide their own integrity, so checksum is a
//!   gossip-plane (unreliable) concern.
//!
//! Each variant is gated on exactly the backend feature the codec gates the
//! algorithm on (`compression-*` / `checksum-*` / `encryption-*`), so a build
//! with only some backends compiles only the reachable combinations.
//!
//! The on-wire byte layout differs from the historical hand-rolled codec; this
//! suite asserts the *property* (faithful decode + transform applied), not byte
//! compatibility with any other implementation.

#![allow(clippy::type_complexity)]

use std::net::SocketAddr;

use bytes::Bytes;
#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3"
))]
use memberlist_proto::ChecksumAlgorithm;
#[cfg(any(
  feature = "lz4",
  feature = "snappy",
  feature = "zstd",
  feature = "brotli"
))]
use memberlist_proto::{CompressAlgorithm, compression::ZstdLevel};
use memberlist_proto::{
  ChecksumOptions, CompressionOptions, EncryptionOptions, decode_incoming,
  encode_checksummed_frame, encode_compressed_frame, encode_encrypted_frame, encode_outgoing,
  encode_outgoing_compound, encode_reliable_unit_with_encryption,
  framing::{MessageTag, encode_compound, encode_message},
  parse_messages, take_reliable_unit_with_encryption,
  typed::{Ack, Alive, Message, Meta, Nack, Node, Ping, PushNodeState, PushPull, State},
  unwrap_transforms_with_encryption,
};
#[cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305"))]
use memberlist_proto::{Keyring, SecretKey};

type I = smol_str::SmolStr;
type A = SocketAddr;

/// The label every "with label" variant isolates traffic under. A non-empty,
/// valid-UTF-8 byte string — the codec only writes a label frame when the
/// label is non-empty.
const LABEL: &[u8] = b"test-cluster";

/// A generous plaintext ceiling for the bomb-guards — comfortably above every
/// payload this suite builds, so a legitimate frame is never rejected as
/// oversize.
const MAX_PLAINTEXT: usize = 16 * 1024 * 1024;

/// A heterogeneous batch of every message *shape*, so the roundtrip exercises
/// each wire tag and a mix of small and large bodies (the large `Alive` /
/// `PushPull` metas push the payload over the default 512-byte compression
/// threshold so the compressed path is genuinely taken).
fn sample_messages() -> Vec<Message<I, A>> {
  let a: A = "127.0.0.1:7946".parse().unwrap();
  let b: A = "10.0.0.2:4567".parse().unwrap();
  let node_a = Node::new(I::from("alpha"), a);
  let node_b = Node::new(I::from("bravo"), b);

  // A meta large enough to clear the compression threshold and to make the
  // "large payload" case non-trivial, but well under `Meta::MAX_SIZE`.
  let big_meta = Meta::try_from(Bytes::from(vec![b'm'; 1024])).expect("meta within bound");

  vec![
    Message::Ack(Ack::new(7)),
    Message::Nack(Nack::new(8)),
    Message::Ping(Ping::new(1, node_a.clone(), node_b.clone())),
    Message::Alive(Alive::new(3, node_a.clone()).with_meta(big_meta.clone())),
    Message::UserData(Bytes::from_static(b"opaque user bytes")),
    Message::PushPull(PushPull::new(
      true,
      [
        PushNodeState::new(5, I::from("alpha"), a, State::Alive).with_meta(big_meta),
        PushNodeState::new(6, I::from("bravo"), b, State::Suspect),
      ]
      .into_iter(),
    )),
  ]
}

/// A single small message — the empty-payload / minimal edge of the matrix
/// (a bare `Ack`, the smallest frame the codec emits).
fn tiny_message() -> Message<I, A> {
  Message::Ack(Ack::new(0))
}

/// The optional label as the codec's [`EncodeOptions`] expect it.
fn label_opt(with_label: bool) -> Option<Bytes> {
  with_label.then(|| Bytes::from_static(LABEL))
}

// ─── gossip/datagram plane ───────────────────────────────────────────────────

/// Encode `msgs` as one gossip datagram with the requested transform stack,
/// then decode it back, returning the recovered messages. A single message
/// uses the plain frame; two or more use the compound frame — the same split
/// the coordinator's outbound path makes.
///
/// Outbound order mirrors the coordinator: frame the bytes, then (compression
/// won ⇒) wrap in `Compressed`, then (checksum on ⇒) wrap in `Checksumed`, then
/// (encryption on ⇒) wrap in `Encrypted`, then label-wrap the whole datagram.
/// Inbound reverses it: strip the label, then strip the
/// `Encrypted`/`Checksumed`/`Compressed` stack, then parse the frame(s).
fn gossip_roundtrip(
  msgs: &[Message<I, A>],
  with_label: bool,
  compression: &CompressionOptions,
  checksum: &ChecksumOptions,
  encryption: &EncryptionOptions,
) -> Result<(Vec<Message<I, A>>, Bytes), String> {
  // Step 1: frame + label. `decode_incoming` later strips the label and yields
  // the inner framed bytes; here we re-wrap those inner bytes in the transform
  // stack, then re-apply the label so the whole datagram is label-isolated (the
  // coordinator's `wrap_label` covers the whole outbound datagram).
  let label = label_opt(with_label);
  let framed: Bytes = if msgs.len() == 1 {
    encode_outgoing(&msgs[0], &Default::default()).map_err(|e| e.to_string())?
  } else {
    encode_outgoing_compound(msgs, &Default::default()).map_err(|e| e.to_string())?
  };

  // Step 2: compression (when enabled and the result shrinks). The wrapper
  // carries the original length so the decoder bounds its output buffer.
  let compressed: Vec<u8> = match compression.apply(&framed).map_err(|e| e.to_string())? {
    memberlist_proto::CompressionOutcome::Compressed(packed) => encode_compressed_frame(
      compression
        .algorithm()
        .expect("Compressed implies an algorithm"),
      framed.len(),
      &packed,
    ),
    memberlist_proto::CompressionOutcome::Plain => framed.to_vec(),
  };

  // Step 3: checksum (when enabled). Wrap the compressed-or-plain bytes in a
  // `[Checksumed]` frame so the on-wire order is `[Checksumed[[Compressed][frame]]]`.
  let checksummed: Vec<u8> = match checksum.algorithm() {
    Some(algo) => encode_checksummed_frame(algo, &compressed).map_err(|e| e.to_string())?,
    None => compressed,
  };

  // Step 4: encryption (when enabled). Encrypt the result so the full on-wire
  // order is `[Encrypted[[Checksumed[[Compressed][frame]]]]]`.
  let payload: Vec<u8> = match encryption.keyring() {
    Some(kr) => {
      let key = kr.primary_ref();
      encode_encrypted_frame(key.algorithm(), key, &checksummed).map_err(|e| e.to_string())?
    }
    None => checksummed,
  };

  // Step 5: label-wrap the whole transformed datagram. The label primitive is
  // not re-exported as a standalone wrapper, so we round-trip the bytes through
  // the codec's own label frame: a present label is prepended as `[12][len][label]`.
  let datagram: Bytes = if let Some(ref lbl) = label {
    let mut out = Vec::with_capacity(2 + lbl.len() + payload.len());
    out.push(memberlist_proto::LABELED_TAG);
    out.push(lbl.len() as u8);
    out.extend_from_slice(lbl);
    out.extend_from_slice(&payload);
    Bytes::from(out)
  } else {
    Bytes::from(payload)
  };

  // Inbound: strip the label, strip the transform stack, parse the frame(s).
  let dec_label = memberlist_proto::DecodeOptions::new(label);
  let inner = decode_incoming(datagram.clone(), &dec_label).map_err(|e| e.to_string())?;
  let stripped = unwrap_transforms_with_encryption(&inner, MAX_PLAINTEXT, encryption)
    .map_err(|e| e.to_string())?
    .into_owned();
  let decoded = parse_messages::<I, A>(Bytes::from(stripped)).map_err(|e| e.to_string())?;
  Ok((decoded, datagram))
}

/// The leading transform-wrapper tag of `payload`, with the label header (if
/// any) stripped first. Used to assert the outermost transform a datagram
/// actually carries on the wire (e.g. `Checksumed` when checksum is the
/// outermost enabled layer, `Encrypted` when encryption wraps it).
fn outer_transform_tag(datagram: &[u8], with_label: bool) -> u8 {
  let body = if with_label {
    // `[LABELED_TAG][len][label..]` — skip the 2-byte header + the label.
    let label_len = datagram[1] as usize;
    &datagram[2 + label_len..]
  } else {
    datagram
  };
  body[0]
}

/// Assert the gossip plane preserves both the heterogeneous batch and the tiny
/// single message under the given transform stack, for both label states.
fn assert_gossip_matrix(
  compression: &CompressionOptions,
  checksum: &ChecksumOptions,
  encryption: &EncryptionOptions,
) {
  for with_label in [false, true] {
    let batch = sample_messages();
    let (back, datagram) = gossip_roundtrip(&batch, with_label, compression, checksum, encryption)
      .unwrap_or_else(|e| panic!("gossip batch (label={with_label}) failed: {e}"));
    assert_eq!(back, batch, "gossip batch mismatch (label={with_label})");
    assert_outer_transform_tag(&datagram, with_label, checksum, encryption);

    let one = vec![tiny_message()];
    let (back, datagram) = gossip_roundtrip(&one, with_label, compression, checksum, encryption)
      .unwrap_or_else(|e| panic!("gossip single (label={with_label}) failed: {e}"));
    assert_eq!(back, one, "gossip single mismatch (label={with_label})");
    assert_outer_transform_tag(&datagram, with_label, checksum, encryption);
  }
}

/// Assert the datagram's outermost transform wrapper matches the enabled stack:
/// `Encrypted` when encryption is on (it is the outermost transform), else
/// `Checksumed` when checksum is on and encryption is off. When neither is on
/// the leading tag is a message / compound / `Compressed` tag and is not
/// asserted here (the compression matrix already covers that).
fn assert_outer_transform_tag(
  datagram: &[u8],
  with_label: bool,
  checksum: &ChecksumOptions,
  encryption: &EncryptionOptions,
) {
  let tag = outer_transform_tag(datagram, with_label);
  if encryption.is_enabled() {
    assert_eq!(
      tag,
      MessageTag::Encrypted as u8,
      "encryption on ⇒ outermost transform tag is Encrypted"
    );
  } else if checksum.is_enabled() {
    assert_eq!(
      tag,
      MessageTag::Checksumed as u8,
      "checksum on (no encryption) ⇒ outermost transform tag is Checksumed"
    );
  }
}

// ─── reliable-stream plane ───────────────────────────────────────────────────

/// Encode `msgs` as one reliable-stream unit (compression + encryption +
/// length prefix), then take it back off the stream buffer, returning the
/// recovered messages and the on-wire unit. Drives the same
/// `encode_reliable_unit_*` helpers the TCP / QUIC reliable bridges use.
///
/// The reliable plane carries no checksum transform: stream transports supply
/// their own end-to-end integrity, so corruption detection lives on the
/// gossip (unreliable) plane only.
fn reliable_roundtrip(
  msgs: &[Message<I, A>],
  compression: &CompressionOptions,
  encryption: &EncryptionOptions,
) -> Result<(Vec<Message<I, A>>, Vec<u8>), String> {
  let framed: Vec<u8> = if msgs.len() == 1 {
    let any = memberlist_proto::message_to_any(&msgs[0]).map_err(|e| e.to_string())?;
    encode_message(&any).map_err(|e| e.to_string())?
  } else {
    let mut anys = Vec::with_capacity(msgs.len());
    for m in msgs {
      anys.push(memberlist_proto::message_to_any(m).map_err(|e| e.to_string())?);
    }
    encode_compound(&anys).map_err(|e| e.to_string())?
  };

  let unit = encode_reliable_unit_with_encryption(compression, encryption, &framed)
    .map_err(|e| e.to_string())?;
  let (plaintext, consumed) = take_reliable_unit_with_encryption(&unit, encryption, MAX_PLAINTEXT)
    .map_err(|e| e.to_string())?
    .ok_or_else(|| "a complete reliable unit was not recognized".to_string())?;
  if consumed != unit.len() {
    return Err(format!(
      "reliable unit consumed {consumed} of {} bytes",
      unit.len()
    ));
  }
  let decoded = parse_messages::<I, A>(Bytes::from(plaintext)).map_err(|e| e.to_string())?;
  Ok((decoded, unit))
}

/// Assert the reliable plane preserves both the heterogeneous batch and the
/// tiny single message under the given transform stack.
fn assert_reliable_matrix(compression: &CompressionOptions, encryption: &EncryptionOptions) {
  let batch = sample_messages();
  let (back, unit) = reliable_roundtrip(&batch, compression, encryption)
    .unwrap_or_else(|e| panic!("reliable batch failed: {e}"));
  assert_eq!(back, batch, "reliable batch mismatch");
  assert_reliable_outer_tag(&unit, encryption);

  let one = vec![tiny_message()];
  let (back, unit) = reliable_roundtrip(&one, compression, encryption)
    .unwrap_or_else(|e| panic!("reliable single failed: {e}"));
  assert_eq!(back, one, "reliable single mismatch");
  assert_reliable_outer_tag(&unit, encryption);
}

/// Assert a reliable unit's payload (after the leading `[unit_len]` varint)
/// leads with `Encrypted` when encryption is on. When encryption is off the
/// leading tag is not asserted here (the reliable plane never wraps in
/// `Checksumed`).
fn assert_reliable_outer_tag(unit: &[u8], encryption: &EncryptionOptions) {
  // Skip the leading `[unit_len]` varint: bytes with bit-7 set are
  // continuation bytes; the first byte with bit-7 clear terminates it.
  let vbytes = unit.iter().position(|b| b & 0x80 == 0).unwrap() + 1;
  let tag = unit[vbytes];
  if encryption.is_enabled() {
    assert_eq!(
      tag,
      MessageTag::Encrypted as u8,
      "encryption on ⇒ reliable payload leads with Encrypted"
    );
  }
}

/// Run the full cross-plane matrix for one `(compression, checksum, encryption)`
/// triple. Checksum is a gossip-plane (unreliable) concern, so the checksum
/// dimension drives the gossip plane only; the reliable plane is exercised
/// without checksum (it carries none).
fn assert_both_planes_with_checksum(
  compression: &CompressionOptions,
  checksum: &ChecksumOptions,
  encryption: &EncryptionOptions,
) {
  assert_gossip_matrix(compression, checksum, encryption);
  assert_reliable_matrix(compression, encryption);
}

/// Run the full cross-plane matrix with checksumming disabled — the shape the
/// pre-checksum variants (plain / compression / encryption / their pairings)
/// exercise.
fn assert_both_planes(compression: &CompressionOptions, encryption: &EncryptionOptions) {
  assert_both_planes_with_checksum(compression, &ChecksumOptions::new(), encryption);
}

// ─── plain (no transform but the optional label) ─────────────────────────────

#[test]
fn plain_and_label_roundtrip() {
  assert_both_planes(&CompressionOptions::new(), &EncryptionOptions::new());
}

// ─── compression — one variant per backend ───────────────────────────────────

/// Build compression options for `algo` with a low threshold so even the tiny
/// single-message datagram is offered to the backend (the don't-expand fallback
/// still leaves an incompressible tiny frame plain, which the decoder accepts).
#[cfg(any(
  feature = "lz4",
  feature = "snappy",
  feature = "zstd",
  feature = "brotli"
))]
fn compress_opts(algo: CompressAlgorithm) -> CompressionOptions {
  CompressionOptions::new()
    .with_algorithm(algo)
    .with_threshold(8)
}

#[cfg(feature = "lz4")]
#[test]
fn compression_lz4_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Lz4),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "snappy")]
#[test]
fn compression_snappy_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Snappy),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "zstd")]
#[test]
fn compression_zstd_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Zstd(ZstdLevel::default())),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "brotli")]
#[test]
fn compression_brotli_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Brotli),
    &EncryptionOptions::new(),
  );
}

// ─── encryption — one variant per backend ────────────────────────────────────

/// A fixed-bytes keyring for `key`. Deterministic so a flake is a real bug, not
/// entropy noise; the nonce is still random per frame inside the encoder.
#[cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305"))]
fn enc_opts(key: SecretKey) -> EncryptionOptions {
  EncryptionOptions::new().with_keyring(Keyring::new(key))
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encryption_aes128_gcm_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::Aes128([0x11; 16])),
  );
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encryption_aes192_gcm_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::Aes192([0x22; 24])),
  );
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encryption_aes256_gcm_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::Aes256([0x33; 32])),
  );
}

#[cfg(feature = "chacha20-poly1305")]
#[test]
fn encryption_chacha20_poly1305_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::ChaCha20Poly1305([0x44; 32])),
  );
}

// ─── compression × encryption (pairwise) ─────────────────────────────────────
//
// One representative compression backend per available encryption backend, and
// vice versa, so every backend is exercised at least once in a combined stack
// without a full Cartesian blow-up. The on-wire order is asserted by the helper
// (encrypt nests outside compress) implicitly: a decode that recovers the
// message proves the inbound strip order matches the outbound wrap order.

#[cfg(all(feature = "lz4", feature = "aes-gcm"))]
#[test]
fn compression_lz4_and_encryption_aes256_gcm_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Lz4),
    &enc_opts(SecretKey::Aes256([0x55; 32])),
  );
}

#[cfg(all(feature = "zstd", feature = "aes-gcm"))]
#[test]
fn compression_zstd_and_encryption_aes128_gcm_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Zstd(ZstdLevel::default())),
    &enc_opts(SecretKey::Aes128([0x66; 16])),
  );
}

#[cfg(all(feature = "snappy", feature = "chacha20-poly1305"))]
#[test]
fn compression_snappy_and_encryption_chacha20_poly1305_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Snappy),
    &enc_opts(SecretKey::ChaCha20Poly1305([0x77; 32])),
  );
}

#[cfg(all(feature = "brotli", feature = "chacha20-poly1305"))]
#[test]
fn compression_brotli_and_encryption_chacha20_poly1305_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Brotli),
    &enc_opts(SecretKey::ChaCha20Poly1305([0x88; 32])),
  );
}

// ─── checksum — one variant per backend ──────────────────────────────────────
//
// Checksum-alone, and its pairings with compression / encryption, on both
// planes. The matrix helpers assert the `Checksumed` tag is present on the wire
// when checksum is the outermost enabled transform.

/// Checksum options for `algo` (enabled). A `None` algorithm leaves the path
/// identity; a `Some` algorithm wraps every payload in a `[Checksumed]` frame.
#[cfg(any(
  feature = "crc32",
  feature = "xxhash32",
  feature = "xxhash64",
  feature = "xxhash3",
  feature = "murmur3"
))]
fn checksum_opts(algo: ChecksumAlgorithm) -> ChecksumOptions {
  ChecksumOptions::new().with_algorithm(algo)
}

#[cfg(feature = "crc32")]
#[test]
fn checksum_crc32_roundtrip() {
  assert_both_planes_with_checksum(
    &CompressionOptions::new(),
    &checksum_opts(ChecksumAlgorithm::Crc32),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "xxhash64")]
#[test]
fn checksum_xxhash64_roundtrip() {
  assert_both_planes_with_checksum(
    &CompressionOptions::new(),
    &checksum_opts(ChecksumAlgorithm::XxHash64),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "murmur3")]
#[test]
fn checksum_murmur3_roundtrip() {
  assert_both_planes_with_checksum(
    &CompressionOptions::new(),
    &checksum_opts(ChecksumAlgorithm::Murmur3),
    &EncryptionOptions::new(),
  );
}

// ─── checksum × compression ──────────────────────────────────────────────────

#[cfg(all(feature = "crc32", feature = "lz4"))]
#[test]
fn checksum_crc32_and_compression_lz4_roundtrip() {
  assert_both_planes_with_checksum(
    &compress_opts(CompressAlgorithm::Lz4),
    &checksum_opts(ChecksumAlgorithm::Crc32),
    &EncryptionOptions::new(),
  );
}

#[cfg(all(feature = "xxhash64", feature = "zstd"))]
#[test]
fn checksum_xxhash64_and_compression_zstd_roundtrip() {
  assert_both_planes_with_checksum(
    &compress_opts(CompressAlgorithm::Zstd(ZstdLevel::default())),
    &checksum_opts(ChecksumAlgorithm::XxHash64),
    &EncryptionOptions::new(),
  );
}

// ─── checksum × encryption ───────────────────────────────────────────────────

#[cfg(all(feature = "crc32", feature = "aes-gcm"))]
#[test]
fn checksum_crc32_and_encryption_aes256_gcm_roundtrip() {
  assert_both_planes_with_checksum(
    &CompressionOptions::new(),
    &checksum_opts(ChecksumAlgorithm::Crc32),
    &enc_opts(SecretKey::Aes256([0xA1; 32])),
  );
}

#[cfg(all(feature = "murmur3", feature = "chacha20-poly1305"))]
#[test]
fn checksum_murmur3_and_encryption_chacha20_poly1305_roundtrip() {
  assert_both_planes_with_checksum(
    &CompressionOptions::new(),
    &checksum_opts(ChecksumAlgorithm::Murmur3),
    &enc_opts(SecretKey::ChaCha20Poly1305([0xA2; 32])),
  );
}

// ─── all transforms together: label × compression × checksum × encryption ─────
//
// The richest stack the codec can express: a label-isolated, compressed,
// checksummed, AND encrypted gossip datagram, plus the
// compressed+checksummed+encrypted reliable unit. Runs only when a compression,
// a checksum, and an encryption backend are all built in.

#[cfg(all(feature = "lz4", feature = "crc32", feature = "aes-gcm"))]
#[test]
fn all_transforms_lz4_crc32_aes256_gcm_roundtrip() {
  let compression = compress_opts(CompressAlgorithm::Lz4);
  let checksum = checksum_opts(ChecksumAlgorithm::Crc32);
  let encryption = enc_opts(SecretKey::Aes256([0x99; 32]));

  // Gossip with the label explicitly on — the all-combined datagram is
  // `[label][Encrypted[[Checksumed[[Compressed][compound frame]]]]]`.
  let batch = sample_messages();
  let (back, datagram) = gossip_roundtrip(&batch, true, &compression, &checksum, &encryption)
    .expect("all-transforms gossip roundtrip");
  assert_eq!(back, batch, "all-transforms gossip batch mismatch");
  assert_eq!(
    datagram[0],
    memberlist_proto::LABELED_TAG,
    "all-transforms datagram must be label-wrapped as a whole"
  );

  // Reliable unit with the same compression+encryption stack — the reliable
  // plane carries no checksum (the gossip plane above covers that dimension).
  assert_reliable_matrix(&compression, &encryption);
}

// ─── corrupted-payload-under-checksum rejection ──────────────────────────────

/// A single payload byte flipped under a `Checksumed` wrapper makes the
/// recomputed digest disagree with the carried digest: the gossip-plane decode
/// must REJECT with a checksum mismatch, not silently surface corrupt bytes.
#[cfg(feature = "crc32")]
#[test]
fn corrupted_checksummed_gossip_is_rejected() {
  let checksum = checksum_opts(ChecksumAlgorithm::Crc32);
  let encryption = EncryptionOptions::new();
  let batch = sample_messages();
  // A clean checksummed (no label, no encryption) gossip datagram so the
  // outermost wrapper is `Checksumed`; flipping a payload byte then breaks the
  // digest the receiver recomputes.
  let (_back, datagram) = gossip_roundtrip(
    &batch,
    false,
    &CompressionOptions::new(),
    &checksum,
    &encryption,
  )
  .expect("clean checksummed gossip roundtrip");
  assert_eq!(
    datagram[0],
    MessageTag::Checksumed as u8,
    "precondition: the datagram is a Checksumed frame"
  );
  // Corrupt the final payload byte (well past the `[tag][algo][digest]` header).
  let mut corrupt = datagram.to_vec();
  let last = corrupt.len() - 1;
  corrupt[last] ^= 0xff;
  let stripped = unwrap_transforms_with_encryption(&corrupt, MAX_PLAINTEXT, &encryption);
  assert!(
    matches!(
      stripped,
      Err(memberlist_proto::FrameError::Checksum(
        memberlist_proto::ChecksumError::Mismatch
      ))
    ),
    "a corrupted checksummed gossip datagram must be rejected as a mismatch, got {stripped:?}"
  );
}

/// A gossip frame at exactly `gossip_mtu`, when both checksummed and encrypted,
/// must round-trip through a decode bounded by `gossip_mtu` — the realistic
/// bomb-guard the drivers pass, not a generous ceiling.
///
/// Checksum nests inside encryption (`compress → checksum → encrypt`), so the
/// decrypted plaintext is the frame plus the checksum wrapper — up to
/// `gossip_mtu + CHECKSUMED_WRAPPER_OVERHEAD`. The decode must allow that slack
/// or a legitimate near-MTU datagram is silently rejected as oversize. This is
/// the regression guard for the encrypted-plaintext bound under-accounting for
/// the checksum layer.
#[cfg(all(feature = "crc32", feature = "aes-gcm"))]
#[test]
fn near_mtu_checksum_and_encryption_gossip_survives_the_bomb_guard() {
  let key = SecretKey::Aes256([0x33; 32]);
  let encryption = EncryptionOptions::new().with_keyring(Keyring::new(key));

  // A UserData message with a large body; its framed size is treated as the
  // gossip MTU so the inner frame sits at exactly the plaintext ceiling.
  let msg = Message::UserData(Bytes::from(vec![0xABu8; 1400]));
  let framed = encode_outgoing(&msg, &Default::default()).expect("frame");
  let gossip_mtu = framed.len();

  // Wrap: checksum (no compression) then encrypt. The checksum wrapper MUST
  // inflate the plaintext past `gossip_mtu` — that is the condition under test.
  let checksummed =
    encode_checksummed_frame(ChecksumAlgorithm::Crc32, &framed).expect("checksum frame");
  assert!(
    checksummed.len() > gossip_mtu,
    "the checksum wrapper must inflate the frame past gossip_mtu (else the test is vacuous)"
  );
  let key_ref = encryption.keyring().unwrap().primary_ref();
  let on_wire =
    encode_encrypted_frame(key_ref.algorithm(), key_ref, &checksummed).expect("encrypt frame");

  // Decode bounded by the realistic `gossip_mtu` the drivers pass — not the
  // generous `MAX_PLAINTEXT` the rest of this suite uses.
  let stripped = unwrap_transforms_with_encryption(&on_wire, gossip_mtu, &encryption)
    .expect("a near-MTU checksummed + encrypted gossip datagram must survive the bomb-guard")
    .into_owned();
  let decoded = parse_messages::<I, A>(Bytes::from(stripped)).expect("parse");
  assert_eq!(
    decoded,
    vec![msg],
    "the near-MTU message must round-trip intact"
  );
}

/// An `Encrypted` frame whose decrypted plaintext is a bare (non-checksum) frame
/// larger than `gossip_mtu` must be rejected: the decrypt slack covers a
/// checksum wrapper, not an over-ceiling plain frame smuggled through it.
#[cfg(feature = "aes-gcm")]
#[test]
fn over_mtu_encrypted_only_gossip_is_rejected() {
  let key = SecretKey::Aes256([0x44; 32]);
  let encryption = EncryptionOptions::new().with_keyring(Keyring::new(key));

  let msg: Message<I, A> = Message::UserData(Bytes::from(vec![0xCDu8; 1400]));
  let framed = encode_outgoing(&msg, &Default::default()).expect("frame");
  // `gossip_mtu` sits just below the frame so the bare frame fits inside the
  // decrypt slack (mtu < len <= mtu + CHECKSUMED_WRAPPER_OVERHEAD) yet exceeds
  // the ceiling — the exit check must still reject it.
  let gossip_mtu = framed.len() - 5;
  let key_ref = encryption.keyring().unwrap().primary_ref();
  let on_wire =
    encode_encrypted_frame(key_ref.algorithm(), key_ref, &framed).expect("encrypt frame");

  let r = unwrap_transforms_with_encryption(&on_wire, gossip_mtu, &encryption);
  assert!(
    matches!(r, Err(memberlist_proto::FrameError::OversizePlaintext(_))),
    "an over-ceiling encrypted-only plain frame must be rejected, got {r:?}"
  );
}

/// A `Checksumed` frame whose verified inner payload is a bare frame larger than
/// `gossip_mtu` must be rejected after the wrapper is stripped — the checksum
/// arm does not get to return an over-ceiling frame to the decoder.
#[cfg(feature = "crc32")]
#[test]
fn over_mtu_checksum_only_gossip_is_rejected() {
  let msg: Message<I, A> = Message::UserData(Bytes::from(vec![0xCDu8; 1400]));
  let framed = encode_outgoing(&msg, &Default::default()).expect("frame");
  // `gossip_mtu` well below the inner frame so the stripped payload is over-ceiling.
  let gossip_mtu = framed.len() - 20;
  let on_wire =
    encode_checksummed_frame(ChecksumAlgorithm::Crc32, &framed).expect("checksum frame");

  // No encryption configured: the checksum arm strips, then the exit check fires.
  let encryption = EncryptionOptions::new();
  let r = unwrap_transforms_with_encryption(&on_wire, gossip_mtu, &encryption);
  assert!(
    matches!(r, Err(memberlist_proto::FrameError::OversizePlaintext(_))),
    "an over-ceiling checksum-only inner frame must be rejected, got {r:?}"
  );
}

/// A datagram with nested (repeated) `Checksumed` wrappers is rejected as
/// non-canonical. Without the canonical-order guard, a peer could nest many
/// publicly-forgeable checksum wrappers in one packet to force thousands of
/// digest passes and buffer copies; the guard bounds the unwrap to O(1).
#[cfg(feature = "crc32")]
#[test]
fn nested_checksum_wrappers_are_rejected() {
  let msg: Message<I, A> = Message::UserData(Bytes::from_static(b"payload"));
  let framed = encode_outgoing(&msg, &Default::default()).expect("frame");
  let once = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &framed).expect("checksum once");
  let twice = encode_checksummed_frame(ChecksumAlgorithm::Crc32, &once).expect("checksum twice");

  let encryption = EncryptionOptions::new();
  let r = unwrap_transforms_with_encryption(&twice, MAX_PLAINTEXT, &encryption);
  assert!(
    matches!(r, Err(memberlist_proto::FrameError::NonCanonicalTransform)),
    "nested Checksumed wrappers must be rejected as non-canonical, got {r:?}"
  );
}
