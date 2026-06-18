//! Generic owned message *shapes* (`Alive<I,A>`, `Suspect<I>`, …).
//!
//! These are the application-facing typed representations consumed by
//! `memberlist-proto`. They carry NO wire codec — the hand-rolled
//! `Data`/`DataRef` message impls from `memberlist-proto` are deliberately
//! left behind (frozen backup). [`crate::bridge`] bridges these to the
//! buffa-generated concrete codec in [`crate::messages`] using the
//! [`crate::data::Data`] trait for the `I`/`A` byte fields.

mod ack;
mod alive;
mod error_response;
mod message;
mod meta;
mod node_state;
mod ping;
mod push_pull;
mod state;
mod suspect_dead;
mod version;

pub use crate::{CheapClone, Node};

pub use ack::{Ack, Nack};
pub use alive::Alive;
pub use error_response::ErrorResponse;
pub use message::{Message, message_tags};
pub use meta::{LargeMeta, Meta};
pub use node_state::NodeState;
pub use ping::{IndirectPing, Ping};
pub use push_pull::{PushNodeState, PushPull};
pub use state::State;
pub use suspect_dead::{Dead, Suspect};
pub use version::{DelegateVersion, ProtocolVersion};

#[cfg(all(test, feature = "std"))]
mod tests;
