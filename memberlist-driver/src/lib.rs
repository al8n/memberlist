#![doc = include_str!("../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/memberlist/main/art/logo_72x72.png")]
pub mod error;
mod observation;
mod snapshot;

pub use observation::observation_payload_bytes;
pub use snapshot::MemberlistSnapshot;
