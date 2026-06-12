//! Thin adapters over `memberlist-wire`'s bridge + plain-frame codec.
//!
//! `crate::typed::Message<I, A>` is a plain owned shape with **no**
//! wire codec (by design — see `memberlist-wire/src/typed.rs`). The on-wire
//! form is `[TAG: 1][VARINT body_len][BODY]`, produced by
//! `crate::framing::encode_message` after bridging the typed
//! message to `framing::AnyMessage` via `crate::message_to_any`.
//! The reverse path is `framing::decode_message` → `message_from_any`.
//!
//! This module is the single place those two hops are composed, so call
//! sites in `endpoint.rs` / `stream.rs` / `broadcast.rs` stay terse and
//! keep the codebase's `.expect("… cannot fail for well-formed data")`
//! convention for messages the machine itself constructed.

#[cfg(not(feature = "std"))]
use std::vec::Vec;

use crate::{Data, framing, message_from_any, message_to_any, typed::Message};

/// Re-exported compound-frame overhead constants (defined in
/// `memberlist-wire::framing`) for the gossip MTU budget in `endpoint.rs`.
pub(crate) use crate::framing::{
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
) -> Result<Vec<u8>, crate::error::StreamError> {
  let any = message_to_any::<I, A>(msg)?;
  Ok(framing::encode_message(&any)?)
}

/// Decode a single plain frame into an owned `typed::Message<I, A>`.
///
/// Returns the message and the number of bytes consumed from `buf`
/// (`== buf.len()` for an exact single-frame buffer). The two distinct wire
/// failures are returned typed — `StreamError::Frame` for the inner-frame codec
/// and `StreamError::Bridge` for the typed-to-buffa bridge — so the FSM keeps
/// the `source()` chain rather than a flattened string.
#[inline]
pub(crate) fn decode_message<I: Data, A: Data>(
  buf: &[u8],
) -> Result<(usize, Message<I, A>), crate::error::StreamError> {
  let (consumed, any) = framing::decode_message(buf)?;
  let msg = message_from_any::<I, A>(&any)?;
  Ok((consumed, msg))
}
