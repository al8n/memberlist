//! Plain-TCP reliable record layer = the cluster-label decorator over a pure
//! byte pipe.
//!
//! The plain-TCP reliable path is [`Labeled`](crate::streams::Labeled) wrapping
//! [`Passthrough`](crate::streams::Passthrough): a one-time `[LABELED_TAG=12][len][label]`
//! cluster-label prefix (byte-compatible with the frozen `memberlist-proto`
//! label frame) written once at stream start, then raw byte passthrough —
//! there is no transport-level encryption. The label step (eager-dialer /
//! lazy-acceptor timing, bidirectional inbound validation, the unlabeled-node
//! passthrough) lives in [`label`](crate::streams::label); the byte pipe and the
//! decorator live in [`labeled`](crate::streams::labeled). TLS's in-band `close_notify`
//! half-close anchor is replaced by the out-of-band TCP FIN
//! (`shutdown(write)`).

use crate::streams::{Labeled, Passthrough};

/// The plain-TCP reliable record layer: the cluster-label decorator over a
/// pure byte pipe.
///
/// Carried by a [`StreamEndpoint`](crate::streams::StreamEndpoint) parameterised with
/// `R = RawRecords`, so reliable membership exchanges flow over a per-exchange
/// plain TCP connection (plain UDP carries the unreliable gossip on a separate
/// socket).
pub type RawRecords = Labeled<Passthrough>;
