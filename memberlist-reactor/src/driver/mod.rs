//! The backend driver poll-pumps, one per reliable plane.
//!
//! [`quic`] owns a `QuicEndpoint` over QUIC datagrams and streams; [`stream`]
//! owns a `StreamEndpoint` over plain-TCP / TLS records. Each is a quinn-style
//! `Future::poll` pump that solely owns its machine endpoint and advances it.

#[cfg(feature = "quic")]
pub(crate) mod quic;
#[cfg(any(feature = "tcp", feature = "tls"))]
pub(crate) mod stream;
