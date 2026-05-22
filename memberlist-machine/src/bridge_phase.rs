//! Transport-agnostic half-close lifecycle for a composed reliable-exchange
//! bridge, shared by the QUIC and TLS coordinators.
//!
//! Anchors are transport-specific: QUIC uses `StreamEvent::Finished` /
//! `Chunks::next() -> Ok(None)`; TLS uses `send_close_notify()` /
//! `IoState::peer_has_closed()` (or a TCP `read == 0`). The phase shape and
//! its monotonic terminality are transport-agnostic.
//!
//! These items are consumed by whichever coordinator is compiled; the
//! `#![allow(dead_code)]` covers a `tls`-only build until the TLS coordinator
//! composes them (the QUIC build already does).
#![allow(dead_code)]

/// Transport lifecycle of one composed `(memberlist Stream, transport bidi)`
/// exchange. Phases are monotonic — once `BothClosed` or `Failed` the
/// bridge is terminal and `pump_bridges` reaps via `drain_then_reap` on
/// its next iteration. Transport invariants live in the phase, not in
/// scattered booleans: a clean reap is `BothClosed` BY CONSTRUCTION
/// (both halves transport-retired) and a failed reap is `Failed(reason)`
/// BY CONSTRUCTION (the failure transition retires both halves
/// explicitly).
///
/// Phase transitions are anchored to transport-specific PUBLIC observables;
/// each coordinator derives its phase from the observable anchors that
/// match its transport (see module-level docs for the per-transport mapping).
/// The conceptual lifecycle is transport-agnostic; the anchors are
/// transport-specific.
#[derive(Debug)]
pub(crate) enum BridgePhase {
  /// Both halves active; bridge is pumping bytes both directions.
  Active,
  /// Send half retired — local FIN ack'd by peer. Recv half still active OR
  /// idle awaiting peer's FIN.
  SendClosed,
  /// Recv half retired — the peer sent FIN and we consumed it. Send half
  /// still flushing OR not yet finish-called.
  RecvClosed,
  /// Both halves transport-retired by their natural FIN paths. Bridge
  /// reaps on the next `pump_bridges` iteration; `drain_then_reap`
  /// delivers `EndpointEvent::StreamClosed` to the FSM.
  BothClosed,
  /// Transport or admission failure — the failure transition retires
  /// both halves explicitly AND clears `pending_out`. Bridge reaps on the
  /// next `pump_bridges`; `drain_then_reap` delivers
  /// `EndpointEvent::StreamErrored{err}`.
  Failed(BridgeFailure),
}

/// Reason the bridge transitioned to [`BridgePhase::Failed`]. Carries
/// through to the `EndpointEvent::StreamErrored` notice delivered to the
/// FSM by `drain_then_reap`, so the FSM's failure attribution is
/// preserved end-to-end.
#[derive(Debug, Clone)]
pub(crate) enum BridgeFailure {
  /// `Bridge::deadline` elapsed while the bridge was non-terminal — the
  /// flush deadline path (post-deadline stale-write prevention).
  Timeout,
  /// The transport returned an error (peer reset, closed stream, illegal
  /// ordered read, blocked-without-credit-recovery, stopped stream, etc.)
  /// from a send or receive operation.
  Transport(String),
  /// `Stream::handle_data` rejected the inbound bytes — memberlist
  /// codec / decode failure on the decoded frame.
  Decode,
  /// The underlying connection was lost. The inline-D1-reap path calls
  /// `bridge.fail(...)` with this variant.
  ConnectionLost,
  /// `Endpoint::handle_stream_event` returned `StreamCommand::Close` —
  /// `MergeDelegate`/`AliveDelegate` admission rejection on an inbound
  /// push/pull exchange. A rejected merge aborts the exchange.
  AdmissionClosed,
  /// The dial intent was retired by the inner endpoint (deadline elapsed
  /// in `Endpoint::dial_succeeded`, or an external `dial_failed` cleared
  /// the intent) before a `Stream` could be minted. Triggered by the
  /// unified `StreamEndpoint::service_handshake_completions`
  /// `dial_succeeded(None)` reap path; runs `clear_outbound` +
  /// `purge_transmit_for` + `purge_pending_connect_for` before dropping
  /// the bridge without `collect_bridge_transmits`.
  DialRetired,
}
