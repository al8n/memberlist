//! Plain-TCP reliable record layer for the generic Sans-I/O stream transport.
//!
//! The plain-TCP reliable path is the cluster-label decorator over a pure byte
//! pipe: [`RawRecords`] is [`crate::streams::Labeled`] wrapping
//! [`crate::streams::Passthrough`] (see [`records`]). A one-time
//! `[LABELED_TAG=12][len][label]` frame (byte-compatible with the frozen
//! `memberlist-proto` label frame) is written once at stream start by both
//! sides, then bytes pass through verbatim — there is no transport-level
//! encryption. A [`crate::streams::StreamEndpoint`] parameterised with
//! `R = RawRecords` carries reliable membership exchanges over a per-exchange
//! plain TCP connection (plain UDP carries the unreliable gossip on a separate
//! socket). TLS's in-band `close_notify` half-close anchor is replaced by the
//! out-of-band TCP FIN (`shutdown(write)`).
//!
//! The plain-TCP options bundle is [`crate::streams::LabelOptions`]`<()>`:
//! construct via [`LabelOptions::new_in`] (panics on invalid label) or
//! [`LabelOptions::try_new_in`] (checked).

#[cfg(all(test, compression, encryption))]
mod bridge;
#[cfg(test)]
mod conn;
mod records;

pub use records::RawRecords;

#[cfg(test)]
mod tests;
