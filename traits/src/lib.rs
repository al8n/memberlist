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

pub trait DelegateManager {
  type Error: std::error::Error + Send + Sync + 'static;

  type Delegate: Delegate<Error = Self::Error> + Send + Sync + 'static;
  type AliveDelegate: AliveDelegate<Error = Self::Error> + Send + Sync + 'static;
  type ConflictDelegate: ConflictDelegate + Send + Sync + 'static;
  type EventDelegate: EventDelegate + Send + Sync + 'static;
  type MergeDelegate: MergeDelegate<Error = Self::Error> + Send + Sync + 'static;
  type PingDelegate: PingDelegate + Send + Sync + 'static;

  fn delegate(&self) -> Option<&Self::Delegate>;
  fn alive_delegate(&self) -> Option<&Self::AliveDelegate>;
  fn conflict_delegate(&self) -> Option<&Self::ConflictDelegate>;
  fn event_delegate(&self) -> Option<&Self::EventDelegate>;
  fn merge_delegate(&self) -> Option<&Self::MergeDelegate>;
  fn ping_delegate(&self) -> Option<&Self::PingDelegate>;
}
