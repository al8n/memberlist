//! `StreamAction` — the high-level driver actions emitted by
//! `StreamEndpoint::poll_action` — plus `ConnectInfo` and `ExchangeRef`.
//!
//! Consumed by the unified stream-transport coordinator.

use core::net::SocketAddr;

use crate::streams::conn::ExchangeId;

/// Payload of [`StreamAction::Connect`]: dial a transport connection for an exchange.
/// Accessor-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectInfo {
  id: ExchangeId,
  peer: SocketAddr,
}

impl ConnectInfo {
  /// The exchange handle the coordinator keys this connection on. Every
  /// subsequent `handle_transport_data` / `poll_transport_transmit` for the
  /// connection carries this same handle.
  pub(crate) const fn new(id: ExchangeId, peer: SocketAddr) -> Self {
    Self { id, peer }
  }

  /// The exchange handle the coordinator keys this connection on. Every
  /// subsequent `handle_transport_data` / `poll_transport_transmit` for the
  /// connection carries this same handle.
  #[inline(always)]
  pub const fn id(&self) -> ExchangeId {
    self.id
  }

  /// The peer `SocketAddr` to connect to.
  #[inline(always)]
  pub const fn peer(&self) -> SocketAddr {
    self.peer
  }
}

/// Payload of [`StreamAction::Shutdown`] / [`StreamAction::Close`]: names one
/// exchange's transport connection. Accessor-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExchangeRef {
  id: ExchangeId,
}

impl ExchangeRef {
  pub(crate) const fn new(id: ExchangeId) -> Self {
    Self { id }
  }

  /// The exchange handle whose transport connection the action refers to.
  #[inline(always)]
  pub const fn id(&self) -> ExchangeId {
    self.id
  }
}

/// A transport directive the coordinator owes the driver for a per-exchange
/// transport connection. Drained via [`crate::streams::StreamEndpoint::poll_action`].
///
/// Newtype variants over named accessor-only payload structs (the
/// no-multi-field-variant convention).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAction {
  /// Open a transport connection to the peer for a freshly-dialed outbound exchange.
  Connect(ConnectInfo),
  /// Half-close the transport write side after the bridge retired its send half,
  /// so the peer reads a clean EOF (its `read == 0`) once it has drained our
  /// buffered bytes.
  Shutdown(ExchangeRef),
  /// Tear down the transport connection and forget the exchange — the bridge
  /// reached a terminal phase and has been reaped.
  Close(ExchangeRef),
}

impl StreamAction {
  /// Borrow the [`ConnectInfo`] iff this is a [`StreamAction::Connect`].
  #[inline(always)]
  pub const fn as_connect(&self) -> Option<&ConnectInfo> {
    match self {
      StreamAction::Connect(c) => Some(c),
      _ => None,
    }
  }

  /// Borrow the [`ExchangeRef`] iff this is a [`StreamAction::Shutdown`].
  #[inline(always)]
  pub const fn as_shutdown(&self) -> Option<&ExchangeRef> {
    match self {
      StreamAction::Shutdown(r) => Some(r),
      _ => None,
    }
  }

  /// Borrow the [`ExchangeRef`] iff this is a [`StreamAction::Close`].
  #[inline(always)]
  pub const fn as_close(&self) -> Option<&ExchangeRef> {
    match self {
      StreamAction::Close(r) => Some(r),
      _ => None,
    }
  }
}
