#![forbid(unsafe_code)]

pub use agnostic;
pub use memberlist_core::*;

/// Re-export of [`memberlist_net`] crate.
#[cfg(feature = "memberlist-net")]
#[cfg_attr(docsrs, doc(cfg(any(feature = "tcp", feature = "tls", feature = "native-tls"))))]
pub mod net {
  pub use memberlist_net::*;
}

/// Re-export of [`memberlist_quic`] crate.
#[cfg(feature = "memberlist-quic")]
#[cfg_attr(docsrs, doc(cfg(any(feature = "quinn", feature = "s2n"))))]
pub mod quic {
  pub use memberlist_quic::*;
}

/// [`Memberlist`](memberlist_core::Memberlist) for `tokio` runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// [`Memberlist`](memberlist_core::Memberlist) for `async-std` runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub mod async_std;

/// [`Memberlist`](memberlist_core::Memberlist) for `smol` runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub mod smol;
