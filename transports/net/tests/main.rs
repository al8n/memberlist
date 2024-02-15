#[path = "main/tests.rs"]
#[macro_use]
mod tests;

#[path = "main/tokio.rs"]
#[cfg(feature = "tokio")]
mod tokio;

#[path = "main/smol.rs"]
#[cfg(feature = "smol")]
mod smol;

#[path = "main/async_std.rs"]
#[cfg(feature = "async-std")]
mod async_std;
