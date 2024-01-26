// #![forbid(unsafe_code)]
// pub use agnostic;
// pub use memberlist_core::*;

// #[cfg(all(feature = "tokio", not(target_family = "wasm")))]
// #[cfg_attr(docsrs, doc(cfg(all(feature = "tokio", not(target_family = "wasm")))))]
// pub type TokioMemberlist<D> = memberlist_core::Memberlist<
//   D,
//   memberlist_core::transport::net::NetTransport<agnostic::tokio::TokioRuntime>,
// >;

// #[cfg(all(feature = "async-std", not(target_family = "wasm")))]
// #[cfg_attr(
//   docsrs,
//   doc(cfg(all(feature = "async-std", not(target_family = "wasm"))))
// )]
// pub type AsyncStdMemberlist<D> = memberlist_core::Memberlist<
//   D,
//   memberlist_core::transport::net::NetTransport<agnostic::async_std::AsyncStdRuntime>,
// >;

// #[cfg(all(feature = "smol", not(target_family = "wasm")))]
// #[cfg_attr(docsrs, doc(cfg(all(feature = "smol", not(target_family = "wasm")))))]
// pub type SmolMemberlist<D> =
//   memberlist_core::Memberlist<D, memberlist_core::transport::net::NetTransport<agnostic::smol::SmolRuntime>>;
