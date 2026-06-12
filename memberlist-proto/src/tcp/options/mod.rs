//! Options bundle for the plain-TCP coordinator.
//!
//! # Cluster isolation on the plain-TCP reliable path
//!
//! The plain-TCP coordinator does not carry a TLS record layer. Cluster
//! isolation is instead provided by the **wire label**: a one-time prefix
//! written at stream start in the same byte format used by the frozen
//! `memberlist-proto` label frame (`[LABELED_TAG=12][len][label_bytes]`,
//! `memberlist/src/codec.rs`). Both the local node and every legitimate peer
//! write that prefix; inbound streams that present a different label (or no
//! label when one is configured) are rejected before any memberlist message
//! is decoded.
//!
//! The plain-TCP spelling of the reliable-plane options is
//! [`LabelOptions<()>`](crate::streams::LabelOptions): it bundles the label and
//! the inbound-check policy over the byte pipe's unit inner options. Construct
//! via [`LabelOptions::try_new_in`] (checked) or [`LabelOptions::new_in`]
//! (panics on an invalid label); the label / skip accessors are on
//! [`LabelOptions`].
//!
//! # Label length and encoding constraints
//!
//! The wire format stores the label length in a single `u8` and reserves two
//! bytes for the `[tag][len]` header, leaving **253 bytes** (`u8::MAX - 2`)
//! for the label itself (`memberlist-proto::Label::MAX_SIZE`,
//! `memberlist/src/codec.rs::MAX_LABEL_LEN`). Labels must also be valid
//! UTF-8 (required by `memberlist-proto::Label` so that labels round-trip
//! through `SmolStr` without loss).

#![cfg(feature = "tcp")]

#[cfg(test)]
mod tests;
