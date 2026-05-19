//! Thin adapters over `memberlist-wire`'s bridge + plain-frame codec.
//!
//! `memberlist_wire::typed::Message<I, A>` is a plain owned shape with **no**
//! wire codec (by design — see `memberlist-wire/src/typed.rs`). The on-wire
//! form is `[TAG: 1][VARINT body_len][BODY]`, produced by
//! `memberlist_wire::framing::encode_message` after bridging the typed
//! message to `framing::AnyMessage` via `memberlist_wire::message_to_any`.
//! The reverse path is `framing::decode_message` → `message_from_any`.
//!
//! This module is the single place those two hops are composed, so call
//! sites in `endpoint.rs` / `stream.rs` / `broadcast.rs` stay terse and
//! keep the codebase's `.expect("… cannot fail for well-formed data")`
//! convention for messages the machine itself constructed.

use memberlist_wire::{Data, framing, message_from_any, message_to_any, typed::Message};

/// Re-exported compound-frame overhead constants (defined in
/// `memberlist-wire::framing`) for the gossip MTU budget in `endpoint.rs`.
pub(crate) use memberlist_wire::framing::{
  COMPOUND_MAX_COUNT_PREFIX_LEN, COMPOUND_MAX_PART_PREFIX_LEN, COMPOUND_TAG_LEN,
};

/// Encode an owned `typed::Message<I, A>` into a plain frame
/// (`[TAG][VARINT len][BODY]`), the exact byte layout the machine's
/// `stream.rs::probe_frame` parses.
///
/// Outbound messages the machine builds itself always bridge successfully
/// (`message_to_any` only fails on missing-required-field / unknown-enum,
/// impossible for well-formed locally-constructed messages), so callers may
/// `.expect()` the result — mirroring the prior
/// `proto::Message::encode_to_vec().expect(...)` convention.
#[inline]
pub(crate) fn encode_message<I: Data, A: Data>(
  msg: &Message<I, A>,
) -> Result<Vec<u8>, memberlist_wire::BridgeError> {
  let any = message_to_any::<I, A>(msg)?;
  framing::encode_message(&any).map_err(|e| {
    memberlist_wire::BridgeError::Encode(memberlist_wire::EncodeError::custom(e.to_string()))
  })
}

/// Decode a single plain frame into an owned `typed::Message<I, A>`.
///
/// Returns the message and the number of bytes consumed from `buf`
/// (`== buf.len()` for an exact single-frame buffer). Errors are returned as
/// a human-readable string so the per-stream FSM can wrap them in
/// `StreamError::Decode` without leaking the two distinct wire error types.
#[inline]
pub(crate) fn decode_message<I: Data, A: Data>(
  buf: &[u8],
) -> Result<(usize, Message<I, A>), String> {
  let (consumed, any) = framing::decode_message(buf).map_err(|e| e.to_string())?;
  let msg = message_from_any::<I, A>(&any).map_err(|e| e.to_string())?;
  Ok((consumed, msg))
}
