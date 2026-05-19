#![doc = include_str!("../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

/// Re-export of [`agnostic`] crate.
pub mod agnostic {
  #[cfg(not(feature = "agnostic"))]
  pub use agnostic_lite::*;

  #[cfg(feature = "agnostic")]
  pub use agnostic::*;
}

// ── Crate surface ─────────────────────────────────────────────────────────
//
// This crate currently exposes the legacy `memberlist-core` passthrough,
// the Sans-I/O state machine (`machine`), and the wire/label `codec`. The
// high-level async driver (`TcpMemberlist` / `QuicMemberlist` and the
// `driver_net` / `driver_quic` / `tls` modules) was removed after the
// compound workstream was adversarially approved; it is being rebuilt as a
// composed QUIC + memberlist super-state-machine in a follow-up PR. Until
// then, drive `machine::Endpoint` with `codec` directly (the deterministic
// `memberlist-simulation` harness shows the pattern).

/// Legacy `Memberlist<T, D>` and `Transport` trait from `memberlist-core`
/// (frozen passthrough, retained for the legacy public API surface).
pub use memberlist_core::*;

/// Re-export of the Sans-I/O state machine crate.
///
/// `memberlist::machine::Endpoint` is the membership/SWIM state machine;
/// construct it directly (paired with `codec`) for unit testing or a
/// custom driver — the forthcoming super-state-machine driver builds on it.
pub mod machine {
  pub use memberlist_machine::*;
}

// The `memberlist-net` / `memberlist-quic` crates were previously
// re-exported here as `net` / `quic_legacy`. They no longer compile against
// `memberlist-machine` and are not a dependency of this crate; they remain
// on disk only as frozen reference (see their BACKUP.md).

/// [`Memberlist`] for `tokio` runtime (legacy core wrapper).
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// [`Memberlist`] for `smol` runtime (legacy core wrapper).
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub mod smol;

// ── Wire codec + runtime shims (the high-level driver was removed; see the
// crate-surface note above — rebuilt next as a super-state-machine) ───────

pub mod codec;
pub mod runtime;
