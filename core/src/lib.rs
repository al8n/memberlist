#![forbid(unsafe_code)]

mod awareness;
mod broadcast;
pub mod error;
mod keyring;
pub use keyring::{SecretKey, SecretKeyring, SecretKeyringError};
pub mod label;
mod network;
mod options;
pub use options::{Options, ShowbizBuilder};
mod queue;
pub use queue::TransmitLimitedQueue;
mod security;
mod showbiz;
mod suspicion;
mod util;

pub use ipnet::IpNet;
pub use showbiz_types::SmolStr;

pub const MIN_PROTOCOL_VERSION: u8 = 1;
pub const PROTOCOL_VERSION2_COMPATIBLE: u8 = 2;
pub const MAX_PROTOCOL_VERSION: u8 = 5;
