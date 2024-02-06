#[path = "main/tests.rs"]
#[macro_use]
mod tests;

#[path = "main/tokio.rs"]
mod tokio;

#[path = "main/smol.rs"]
mod smol;

#[path = "main/async_std.rs"]
mod async_std;
