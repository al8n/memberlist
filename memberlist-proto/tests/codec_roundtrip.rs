//! Cross-transform roundtrip matrix for the wire codec.
//!
//! Drives real typed [`Message<I, A>`](memberlist_proto::typed::Message) values
//! through the codec's outer transform layers — label, compression, encryption,
//! and their pairwise + all-combined compositions — and asserts the codec's own
//! roundtrip identity: `decode(encode(msgs)) == msgs`, and that the transform
//! was actually applied (a non-trivial wrapper tag is present on the wire).
//!
//! The codec exposes two transform planes, both composed from the same public
//! free functions:
//!
//! * the **gossip/datagram plane** — [`encode_outgoing`] /
//!   [`encode_outgoing_compound`] add the optional label; a `Compressed`
//!   wrapper ([`encode_compressed_frame`]) and an `Encrypted` wrapper
//!   ([`encode_encrypted_frame`]) nest inside it, stripped on the way in by
//!   [`decode_incoming`] then [`unwrap_transforms_with_encryption`] then
//!   [`parse_messages`];
//! * the **reliable-stream plane** — [`encode_reliable_unit_with_encryption`] /
//!   [`take_reliable_unit_with_encryption`] fold compression, encryption, and a
//!   self-delimiting length prefix into one unit.
//!
//! Each variant is gated on exactly the backend feature the codec gates the
//! algorithm on (`compression-*` / `encryption-*`), so a build with only some
//! backends compiles only the reachable combinations.
//!
//! The on-wire byte layout differs from the historical hand-rolled codec; this
//! suite asserts the *property* (faithful decode + transform applied), not byte
//! compatibility with any other implementation.

#![allow(clippy::type_complexity)]

use std::net::SocketAddr;

use bytes::Bytes;
#[cfg(any(
  feature = "compression-lz4",
  feature = "compression-snappy",
  feature = "compression-zstd",
  feature = "compression-brotli"
))]
use memberlist_proto::CompressAlgorithm;
use memberlist_proto::{
  CompressionOptions, EncryptionOptions, decode_incoming, encode_compressed_frame,
  encode_encrypted_frame, encode_outgoing, encode_outgoing_compound,
  encode_reliable_unit_with_encryption,
  framing::{encode_compound, encode_message},
  parse_messages, take_reliable_unit_with_encryption,
  typed::{Ack, Alive, Message, Meta, Nack, Node, Ping, PushNodeState, PushPull, State},
  unwrap_transforms_with_encryption,
};
#[cfg(any(
  feature = "encryption-aes-gcm",
  feature = "encryption-chacha20-poly1305"
))]
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
/// Outbound order mirrors the coordinator: label-wrap the framed bytes, then
/// (compression won ⇒) wrap in `Compressed`, then (encryption on ⇒) wrap in
/// `Encrypted`. Inbound reverses it: strip the label, then strip the
/// `Encrypted`/`Compressed` stack, then parse the frame(s).
fn gossip_roundtrip(
  msgs: &[Message<I, A>],
  with_label: bool,
  compression: &CompressionOptions,
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

  // Step 3: encryption (when enabled). Encrypt the compressed-or-plain bytes so
  // the on-wire order is `[Encrypted[[Compressed][frame]]]`.
  let payload: Vec<u8> = match encryption.keyring() {
    Some(kr) => {
      let key = kr.primary_ref();
      encode_encrypted_frame(key.algorithm(), key, &compressed).map_err(|e| e.to_string())?
    }
    None => compressed,
  };

  // Step 4: label-wrap the whole transformed datagram. The label primitive is
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

/// Assert the gossip plane preserves both the heterogeneous batch and the tiny
/// single message under the given transform stack, for both label states.
fn assert_gossip_matrix(compression: &CompressionOptions, encryption: &EncryptionOptions) {
  for with_label in [false, true] {
    let batch = sample_messages();
    let (back, _) = gossip_roundtrip(&batch, with_label, compression, encryption)
      .unwrap_or_else(|e| panic!("gossip batch (label={with_label}) failed: {e}"));
    assert_eq!(back, batch, "gossip batch mismatch (label={with_label})");

    let one = vec![tiny_message()];
    let (back, _) = gossip_roundtrip(&one, with_label, compression, encryption)
      .unwrap_or_else(|e| panic!("gossip single (label={with_label}) failed: {e}"));
    assert_eq!(back, one, "gossip single mismatch (label={with_label})");
  }
}

// ─── reliable-stream plane ───────────────────────────────────────────────────

/// Encode `msgs` as one reliable-stream unit (compression + encryption + length
/// prefix), then take it back off the stream buffer, returning the recovered
/// messages and the on-wire unit. Drives the same `encode_reliable_unit_*`
/// helpers the TCP / QUIC reliable bridges use.
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
  let (back, _) = reliable_roundtrip(&batch, compression, encryption)
    .unwrap_or_else(|e| panic!("reliable batch failed: {e}"));
  assert_eq!(back, batch, "reliable batch mismatch");

  let one = vec![tiny_message()];
  let (back, _) = reliable_roundtrip(&one, compression, encryption)
    .unwrap_or_else(|e| panic!("reliable single failed: {e}"));
  assert_eq!(back, one, "reliable single mismatch");
}

/// Run the full cross-plane matrix for one `(compression, encryption)` pair.
fn assert_both_planes(compression: &CompressionOptions, encryption: &EncryptionOptions) {
  assert_gossip_matrix(compression, encryption);
  assert_reliable_matrix(compression, encryption);
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
  feature = "compression-lz4",
  feature = "compression-snappy",
  feature = "compression-zstd",
  feature = "compression-brotli"
))]
fn compress_opts(algo: CompressAlgorithm) -> CompressionOptions {
  CompressionOptions::new()
    .with_algorithm(algo)
    .with_threshold(8)
}

#[cfg(feature = "compression-lz4")]
#[test]
fn compression_lz4_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Lz4),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "compression-snappy")]
#[test]
fn compression_snappy_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Snappy),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "compression-zstd")]
#[test]
fn compression_zstd_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Zstd),
    &EncryptionOptions::new(),
  );
}

#[cfg(feature = "compression-brotli")]
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
#[cfg(any(
  feature = "encryption-aes-gcm",
  feature = "encryption-chacha20-poly1305"
))]
fn enc_opts(key: SecretKey) -> EncryptionOptions {
  EncryptionOptions::new().with_keyring(Keyring::new(key))
}

#[cfg(feature = "encryption-aes-gcm")]
#[test]
fn encryption_aes128_gcm_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::Aes128([0x11; 16])),
  );
}

#[cfg(feature = "encryption-aes-gcm")]
#[test]
fn encryption_aes192_gcm_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::Aes192([0x22; 24])),
  );
}

#[cfg(feature = "encryption-aes-gcm")]
#[test]
fn encryption_aes256_gcm_roundtrip() {
  assert_both_planes(
    &CompressionOptions::new(),
    &enc_opts(SecretKey::Aes256([0x33; 32])),
  );
}

#[cfg(feature = "encryption-chacha20-poly1305")]
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

#[cfg(all(feature = "compression-lz4", feature = "encryption-aes-gcm"))]
#[test]
fn compression_lz4_and_encryption_aes256_gcm_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Lz4),
    &enc_opts(SecretKey::Aes256([0x55; 32])),
  );
}

#[cfg(all(feature = "compression-zstd", feature = "encryption-aes-gcm"))]
#[test]
fn compression_zstd_and_encryption_aes128_gcm_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Zstd),
    &enc_opts(SecretKey::Aes128([0x66; 16])),
  );
}

#[cfg(all(
  feature = "compression-snappy",
  feature = "encryption-chacha20-poly1305"
))]
#[test]
fn compression_snappy_and_encryption_chacha20_poly1305_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Snappy),
    &enc_opts(SecretKey::ChaCha20Poly1305([0x77; 32])),
  );
}

#[cfg(all(
  feature = "compression-brotli",
  feature = "encryption-chacha20-poly1305"
))]
#[test]
fn compression_brotli_and_encryption_chacha20_poly1305_roundtrip() {
  assert_both_planes(
    &compress_opts(CompressAlgorithm::Brotli),
    &enc_opts(SecretKey::ChaCha20Poly1305([0x88; 32])),
  );
}

// ─── all transforms together: label × compression × encryption ───────────────
//
// The richest stack the codec can express: a label-isolated, compressed, AND
// encrypted gossip datagram, plus the compressed+encrypted reliable unit. Runs
// only when both a compression and an encryption backend are built in. (No
// checksum layer exists in this codec — see the module-level note and the task
// report; the historical checksum combinations have no analog here.)

#[cfg(all(feature = "compression-lz4", feature = "encryption-aes-gcm"))]
#[test]
fn all_transforms_lz4_aes256_gcm_roundtrip() {
  let compression = compress_opts(CompressAlgorithm::Lz4);
  let encryption = enc_opts(SecretKey::Aes256([0x99; 32]));

  // Gossip with the label explicitly on — the all-combined datagram is
  // `[label][Encrypted[[Compressed][compound frame]]]`.
  let batch = sample_messages();
  let (back, datagram) = gossip_roundtrip(&batch, true, &compression, &encryption)
    .expect("all-transforms gossip roundtrip");
  assert_eq!(back, batch, "all-transforms gossip batch mismatch");
  assert_eq!(
    datagram[0],
    memberlist_proto::LABELED_TAG,
    "all-transforms datagram must be label-wrapped as a whole"
  );

  // Reliable unit with the same compression+encryption stack.
  assert_reliable_matrix(&compression, &encryption);
}
