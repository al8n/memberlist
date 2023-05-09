#![forbid(unsafe_code)]

#[cfg(all(feature = "sync", feature = "async"))]
compile_error!("feature `sync` and `async` cannot be enabled at the same time");

#[cfg(feature = "async")]
pub use async_trait;

mod transport;
pub use transport::*;

mod alive_delegate;
pub use alive_delegate::*;

mod broadcast;
pub use broadcast::*;

mod conflict_delegate;
pub use conflict_delegate::*;

mod delegate;
pub use delegate::*;

mod event_delegate;
pub use event_delegate::*;

mod merge_delegate;
pub use merge_delegate::*;

mod ping_delegate;
pub use ping_delegate::*;
