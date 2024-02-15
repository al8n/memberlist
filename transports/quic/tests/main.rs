#[path = "main/tests.rs"]
mod tests;

#[cfg(feature = "quinn")]
#[path = "main/quinn.rs"]
mod quinn;

#[cfg(feature = "s2n")]
#[path = "main/s2n.rs"]
mod s2n;
