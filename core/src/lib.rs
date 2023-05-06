mod awareness;
mod broadcast;
pub mod error;
mod keyring;
mod network;
mod options;
mod queue;
mod security;
mod showbiz;
mod suspicion;
mod util;

pub const MIN_PROTOCOL_VERSION: u8 = 1;
pub const PROTOCOL_VERSION2_COMPATIBLE: u8 = 2;
pub const MAX_PROTOCOL_VERSION: u8 = 5;
